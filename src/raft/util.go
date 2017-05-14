package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
	//"fmt"
	//"os"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}

	return
}


//type logCache struct {
//	mu sync.Mutex
//	buf []string
//}
//
//var logbuf logCache
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		s := fmt.Sprintf(format, a...)
//		logbuf.mu.Lock()
//		logbuf.buf = append(logbuf.buf, s)
//		logbuf.mu.Unlock()
//	}
//
//	return
//}
//
//func InitLog() {
//	logbuf.buf = make([]string, 0)
//}
//
//func OutputLog() {
//	logbuf.mu.Lock()
//	for _, item := range logbuf.buf {
//		fmt.Fprint(os.Stderr, item)
//	}
//	logbuf.mu.Unlock()
//}

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

func (timer *raftTimer) reset() {
	timer.mu.Lock()
	if !timer.t.Stop() {
		select {
		case <-timer.t.C:
		default:
		}
	}
	timer.t.Reset(timer.d)
	timer.mu.Unlock()
}
