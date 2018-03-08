package main

import (
	"log"
	"time"
)

type Rate struct {
	Count int64
	Start time.Time
}

func (r *Rate) Increment() {
	r.Count++
}

func (r *Rate) Print() {
	r.Increment()
	log.Printf("%0.0f req/s", float64(r.Count)/time.Since(r.Start).Seconds())
}

func makeRate() *Rate {
	return &Rate{
		Count: 0,
		Start: time.Now(),
	}
}

func main() {
	rate := makeRate()
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		rate.Print()
	}
}
