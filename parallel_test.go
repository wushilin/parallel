package parallel

import (
	"fmt"
	//"stream"
	"testing"
	"time"
	parallel "."
	"math/rand"
)

func TestExecutor(*testing.T) {
	p := parallel.NewExecutor(1000, 10000)
	p.Start()
	fs := make([]parallel.Future, 1000)
	for i := 0; i < 1000; i++ {
		f := p.Submit(func() interface{} {
			//ßfmt.Println("Starting execution")
			defer func() {
				//fmt.Println("Ending execution")
			}()

			time.Sleep(time.Duration(rand.Int()%5000+1000) * time.Millisecond)
			return 5
		})
		fs[i] = f
	}
	p.Stop()

	sum := 0
	dc := 0
	added := make([]bool, 1000)
	for {

		for idx, v := range fs {
			if added[idx] {
				continue
			}
			if v.Ready() {
				dc++
				sum += v.Wait().(int)
				added[idx] = true
			}
		}
		if dc == 1000 {
			break
		} else {
			fmt.Println(p.Active(), p.Completed(), p.Submited(), p.Running(), p.Pending())
			time.Sleep(time.Second)
		}
	}
	fmt.Println("Sum is:", sum)
}

func print(i interface{}) {
	fmt.Println(i)
}
func makeFunc() func() interface{} {
	return func() interface{} {
		time.Sleep(5000 * time.Millisecond)
		return 5
	}
}
