package spmc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPoolDequeue(t *testing.T) {
	const P = 10
	// In long mode, do enough pushes to wrap around the 21-bit
	// indexes.
	N := 1<<21 + 1000
	if testing.Short() {
		N = 1e3
	}
	d := NewPoolDequeue[int](16)
	have := make([]int32, N)
	var stop int32
	var wg sync.WaitGroup

	// Start P-1 consumers.
	for i := 1; i < P; i++ {
		wg.Add(1)
		go func() {
			fail := 0
			for atomic.LoadInt32(&stop) == 0 {
				val, ok := d.PopTail()
				if ok {
					//log.Println("pop tail", val, ok)
					fail = 0
					atomic.AddInt32(&have[val], 1)
					if val == N-1 {
						atomic.StoreInt32(&stop, 1)
					}
				} else {
					// Speed up the test by
					// allowing the pusher to run.
					if fail++; fail%100 == 0 {
						runtime.Gosched()
					}
				}
			}
			wg.Done()
		}()
	}

	// Start 1 producer.
	nPopHead := 0
	wg.Add(1)
	go func() {
		for j := 0; j < N; j++ {
			for !d.PushHead(j) {
				// Allow a popper to run.
				runtime.Gosched()
			}
			if j%10 == 0 {
				val, ok := d.PopHead()
				if ok {
					nPopHead++
					atomic.AddInt32(&have[val], 1)
				}
			}
		}
		wg.Done()
	}()
	wg.Wait()

	// Check results.
	for i, count := range have {
		if count != 1 {
			t.Errorf("expected have[%d] = 1, got %d", i, count)
		}
	}
	if nPopHead == 0 {
		// In theory it's possible in a valid schedule for
		// popHead to never succeed, but in practice it almost
		// always succeeds, so this is unlikely to flake.
		t.Errorf("popHead never succeeded")
	}
}

func BenchmarkChannelSPMC(b *testing.B) {
	ch := make(chan interface{}, 128)
	var wg sync.WaitGroup
	wg.Add(1000)
	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		for i := 0; i < b.N; i++ {
			ch <- 100
		}
	}()

	for i := 0; i < 1000; i++ {
		go func() {
			for i := 0; i < b.N/1000; i++ {
				<-ch
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkSPMC(b *testing.B) {
	pool := NewPoolChain[int]()
	var wg sync.WaitGroup
	wg.Add(1000)
	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		for i := 0; i < b.N; i++ {
			pool.PushHead(100)
		}
	}()

	for i := 0; i < 1000; i++ {
		go func() {
			for i := 0; i < b.N/1000; i++ {
				_, ok := pool.PopTail()
				_ = ok
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
