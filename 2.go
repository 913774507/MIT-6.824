package main

import (
	"fmt"
	"math/rand"
	"time"
)

type A struct {
	mp map[interface{}]bool
}

const electionDuration int64 = 500

func getRandomTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randTime := rand.Int63() % 500
	return time.Duration((randTime + electionDuration) * int64(time.Millisecond))
}

func main() {
	// a := &A{}
	// // a.mp = map[interface{}]bool{}
	// a.mp = make(map[interface{}]bool, 0)
	// fmt.Println(a.mp)
	// a.mp['a'] = true
	// fmt.Println(a.mp)

	// for i := 0; i < 10; i++ {
	// 	t := rand.Int63()
	// 	fmt.Println(t)
	// }
	// rand.Seed(time.Now().UnixNano())
	// t := rand.Int() % 100
	// fmt.Printf("rand %v\n", t)
	// go func() {
	// 	i := 0
	// 	for {
	// 		i++
	// 		time.Sleep(time.Second)
	// 		fmt.Println(i)
	// 	}
	// }()
	// timer := time.NewTimer(1 * time.Second)
	// time.Sleep(2 * time.Second)
	// // for t := range timer.C {
	// // 	fmt.Println(t)
	// // }
	// t := <-timer.C
	// fmt.Println("first timer:", t)

	// time.Sleep(5 * time.Second)
	// timer.Reset(1 * time.Second)
	// t = <-timer.C
	// fmt.Println("second timer:", t)

	for i := 0; i < 5; i++ {
		t := getRandomTime()
		fmt.Println(t)
	}
}
