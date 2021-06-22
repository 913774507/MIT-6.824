package main

// func f(wg *sync.WaitGroup, arg int, ch chan interface{}) {
// 	arg += 1
// 	ch <- arg
// 	fmt.Printf("arg %v into ch\n", arg)
// 	wg.Done()
// }

// func f(arg *int, ch chan *int) {
// 	time.Sleep(1 * time.Second)
// 	*arg += 1
// 	ch <- arg
// 	r := <-ch
// 	fmt.Printf("r %v\n", *r)
// }

func main() {
	// var wg sync.WaitGroup
	// wg.Add(5)
	// for i := 0; i < 5; i++ {
	// 	go f(arg, ch)
	// }
	// wg.Wait()
	// close(ch)
	// for i := range ch{
	// 	fmt.Println(i)
	// }

	// for {
	// 	select {
	// 	case i := <-ch:
	// 		fmt.Println(i)
	// 	}
	// }

}
