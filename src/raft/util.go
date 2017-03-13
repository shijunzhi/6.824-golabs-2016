package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randomInt(min, max int) int {
	var n int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		n = r.Intn(max)
		if n > min {
			break
		}
	}
	return n
}

type raftTimer struct {
	mu sync.Mutex
	t  *time.Timer
	d  time.Duration
}

func newRaftTimer(interval time.Duration) *raftTimer {
	timer := &raftTimer{d: interval}
	timer.t = time.NewTimer(timer.d)
	if !timer.t.Stop() {
		<-timer.t.C
	}
	return timer
}

func (timer *raftTimer) stop() {
	timer.mu.Lock()
	if !timer.t.Stop() {
		select {
		case <-timer.t.C:
		default:
		}
	}
	timer.mu.Unlock()
}

func (timer *raftTimer) reset(interval time.Duration) {
	timer.mu.Lock()
	if !timer.t.Stop() {
		select {
		case <-timer.t.C:
		default:
		}
	}
	if interval != 0 {
		timer.d = interval
	}
	timer.t.Reset(timer.d)
	timer.mu.Unlock()
}
