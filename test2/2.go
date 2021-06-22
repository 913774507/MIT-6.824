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
	
}
