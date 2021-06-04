package main

import "fmt"

// func f(wg *sync.WaitGroup, arg int, ch chan interface{}) {
// 	arg += 1
// 	ch <- arg
// 	fmt.Printf("arg %v into ch\n", arg)
// 	wg.Done()
// }

// func f(arg int, ch chan interface{}) {
// 	arg += 1
// 	ch <- arg
// 	fmt.Printf("arg %v into ch\n", arg)
// }

// func main() {
// 	arg := 0
// 	ch := make(chan interface{}, 5)
// 	// var wg sync.WaitGroup
// 	// wg.Add(5)
// 	for i := 0; i < 5; i++ {
// 		go f(arg, ch)
// 	}
// 	// wg.Wait()
// 	// close(ch)
// 	// for i := range ch{
// 	// 	fmt.Println(i)
// 	// }

// 	for {
// 		select {
// 		case i := <-ch:
// 			fmt.Println(i)
// 		}
// 	}
// }

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Err         bool
	Server      int
}

func main() {
	r := &RequestVoteReply{}
	fmt.Println(r.VoteGranted)
}
