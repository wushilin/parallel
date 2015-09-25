/*
Provide parallel executor service
*/
package parallel

import (
	"sync"
	"sync/atomic"
	"time"
)

/*
 It is like a map function, but produce no output
a->fmt.Println(a) is a MapCallFunc
*/
type MapCallFunc func(x interface{})

// A Map func tranform value
type MapFunc func(x interface{}) interface{}

// A void func is a func that takes no argument, and returns nothing
type VoidFunc func()

// ProducerFunc is a func that produces a result
type ProducerFunc func() interface{}

// Future represent a Value to be retrieved, and might be already ready, 
// or will be ready in the future.
type Future interface {
	// Test wehther the value is ready
	Ready() bool

	/*
	Wait for max of timeout milliseconds
	for a value and return whether a value present or not
	If second return value is false, the value returned 
	should be nil. Do not use the value unless the second 
	value is true
	*/
	WaitT(timeoutms int) (interface{}, bool)

	// Wait indefinitely for the value
	Wait() interface{}
}

/*
A FutureCall represent a future completion state of a Call.
Call Ready() to determin if he Call is done
Call WaitT(xxx int) to check if the call is complete after the timeout
Note if the call returns earlier than timeout, the WaitT will return immediately
Call Wait() to wait indefinitely for the call to finish.
*/
type FutureCall interface {
	// test whether the future is ready
	Ready() bool
	// Wait max X ms, return true if the call is done. Otherwise return false
	WaitT(timeoutms int) bool
	// Wait until the call is done
	Wait()
}

// A future for concept
type baseFuture struct {
	mutex     *sync.Mutex
	channel   chan interface{}
	valueRead bool
	value     interface{}
}

func (v *baseFuture) WaitT(timeoutms int) (interface{}, bool) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if v.valueRead {
		return v.value, true
	}
	timeout := time.After(time.Duration(timeoutms) * time.Millisecond)
	select {
	case <-timeout:
		return nil, false
	case result := <-v.channel:
		v.valueRead = true
		v.value = result
		return result, true
	}
}

func (v *baseFuture) Wait() interface{} {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	if v.valueRead {
		return v.value
	}
	result := <-v.channel
	v.valueRead = true
	v.value = result
	return result
}

func (v *baseFuture) Ready() bool {
	if v.valueRead {
		return true
	}
	return len(v.channel) > 0
}

type baseFutureCall struct {
	src Future
}

func (v *baseFutureCall) WaitT(timeoutms int) bool {
	_, ok := v.src.WaitT(timeoutms)
	return ok
}

func (v *baseFutureCall) Ready() bool {
	return v.src.Ready()
}

func (v *baseFutureCall) Wait() {
	v.src.Wait()
	return
}

// Apply function f with argument argument
// But will not run it. Instead, it return the future immediately
// and a VoidFunc (func (){}). Caller can call the VoidFunc() to 
// Cause the Future to materialize.
// Example:
// fut, f := MakeFuturePipe(func(i interface{}) interface{} {
//    return i.(int) + 1
// }, 6)
// go f() <= this will start the actual func - without it the fut will never be ready
// fmt.Println(fut.Wait())
// Note the f() is required before the fut.Wait() 
func MakeFuturePipe(f MapFunc, argument interface{}) (Future, VoidFunc) {
	c := make(chan interface{}, 1)
	ff := func() {
		result := f(argument)
		c <- result
		close(c)
	}
	/*
		mutex *sync.Mutex
		channel chan interface{}
		valueRead bool
		value interface{}
	*/
	return &baseFuture{new(sync.Mutex), c, false, nil}, ff
}

/* 
Make a function call as a future. A separate goroutine will be started to execute the code
It is the same as:
  fut, pipe := MakeFuturePipe(f, argument)
  go pipe()

You can retrieve the result Future in caller goroutine sometime in the future
*/
func MakeFuture(f MapFunc, argument interface{}) Future {
	result, pipe := MakeFuturePipe(f, argument)
	go pipe()
	return result
}

/*
Same as MakeFuturePipe, but the result FutureCall doesn't carry value
In fact it is impemented using MakeFuturePipe with wrapper func
*/
func MakeFutureCallPipe(f MapCallFunc, argument interface{}) (FutureCall, VoidFunc) {
	fff, p := MakeFuturePipe(func(a interface{}) interface{} {
		f(a)
		return nil
	}, argument)
	return &baseFutureCall{fff}, p
}

/*
Same as MakeFuture, but the result FutureCall doesn't carry value
In fact it is impemented using MakeFutureCallPipe with wrapper func
*/
func MakeFutureCall(f MapCallFunc, argument interface{}) FutureCall {
	result, pipe := MakeFutureCallPipe(f, argument)
	go pipe()
	return result
}

// A Parallel Executor is an executor that can execute tasks in parallel
type ParallelExecutor interface {
	/*
	Start processing jobs. Note that jobs can be added before Start()
	but without start, Future will never materialize
	You can still add jobs after started. You can start before adding jobs even
	*/
	Start()

	/*
	Submit a producer func and get the result.
	The Future may be ready in the future. Or might be ready when you use it
	call result.Ready() to check whether it is completed
	call result.WaitT to wait for completion, with a timeout
	call result.Wait to wait forever for completion
	*/
	Submit(f ProducerFunc) Future

	/*
	Submit a void func for executing. 
	The result represent a Future Completion status of the VoidFunc
	call result.Ready() to check whether it is completed
	call result.WaitT to wait for completion, with a timeout
	call result.Wait to wait forever for completion
	*/
	Call(f VoidFunc) FutureCall

	/*
	Stop accepting new calls, but all submitted tasks will be executed
	It is essentially closing the task queue channel
	*/
	Stop()

	/*
	Wait until all calls/funcs are done
	You must call Stop() first otherwise the goroutines will never end
	*/
	Wait()

	/*
	Active threads.
	Before start, it should be 0
	After start, it will quickly jump to Parallel settings
	After stop, it will slowly drop to 0.
	Use as a indicator only, don't assume 0 means the 
	Executor done all jobs. Instead, use Stop() and Wait()
	*/
	Active() int

	// Pending jobs count. It is len(queue)
	Pending() int

	// Return parallel configuration. You can't adjust the threads dynamically
	Parallel() int

	// Test wehther the executor is running
	Running() bool

	// Total submited tasks
	Submited() int

	// Number of tasks had been completed
	Completed() int
}

type basePE struct {
	// Mutex for PE modifications
	mutex     *sync.Mutex
	wg        *sync.WaitGroup // used for wait()
	started   bool            // whether this had been started or not
	running   int32           // running tasks
	queue     chan VoidFunc   // task queue
	parallel  int             // parallel execution units
	submitted int
	completed int
}

func (v *basePE) Start() {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if v.started {
		// already started
		return
	}
	cs := make(chan bool, v.parallel)
	for i := 0; i < v.parallel; i++ {
		go func(vv *basePE) {
			vv.wg.Add(1)
			atomic.AddInt32(&vv.running, 1)
			cs <- true
			defer func() {
				atomic.AddInt32(&vv.running, -1)
				vv.wg.Done()
			}()
			for vf := range vv.queue {
				vf()
				vv.completed++
			}
		}(v)
	}
	for i := 0; i < v.parallel; i++ {
		<-cs
	}
	v.started = true
}

func (v *basePE) Submit(f ProducerFunc) Future {
	v.mutex.Lock()

	result, pipe := MakeFuturePipe(func(dummy interface{}) interface{} {
		return f()
	}, nil)

	v.submitted++
	v.mutex.Unlock()
	v.queue <- pipe
	return result
}

func (v *basePE) Call(f VoidFunc) FutureCall {
	v.mutex.Lock()

	result, pipe := MakeFutureCallPipe(func(dummy interface{}) {
		f()
	}, nil)
	v.queue <- pipe
	v.mutex.Unlock()
	return result
}

func (v *basePE) Stop() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	close(v.queue)
}

func (v *basePE) Wait() {
	v.wg.Wait()
}

func (v *basePE) Active() int {
	return int(v.running)
}

func (v *basePE) Pending() int {
	return len(v.queue)
}

func (v *basePE) Parallel() int {
	return v.parallel
}

func (v *basePE) Running() bool {
	return v.running > 0
}

func (v *basePE) Submited() int {
	return v.submitted
}

func (v *basePE) Completed() int {
	return v.completed
}

/*
Return a parallel executor
p is number of concurrent goroutines. Don't set too high or your machine will hang?
qlength is the queue length. If this is small, when caller want to submit
a task, but the queue is full, the caller will be blocked until a slot is available

After executor is closed, pending caller in submission queue will see panic
But already successfully submitted tasks will be executed. All Future and FutureCall
will guarantee a termination with result.
*/
func NewExecutor(p int, qlength int) ParallelExecutor {
	q := make(chan VoidFunc, qlength)

	return &basePE{new(sync.Mutex), new(sync.WaitGroup), false, 0, q, p, 0, 0}
}
