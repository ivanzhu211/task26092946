package taxvault

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPerfRotationUnderLoad 验收规模测试：
// 10 万条客户税号，轮换期间 50 个调用方持续读写，
// 读写 P95 不超过 200ms；停止业务写入后轮换在 60 秒内完成。
func TestPerfRotationUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in -short mode")
	}
	s, _, keyDir := openTemp(t)
	defer s.Close()

	const n = 100000
	seedStart := time.Now()
	for i := 0; i < n; i++ {
		if err := s.Save(fmt.Sprintf("cust-%06d", i), fmt.Sprintf("91%013d%04x", i, i*7)); err != nil {
			t.Fatalf("seed Save: %v", err)
		}
	}
	t.Logf("seed %d records in %v", n, time.Since(seedStart))

	const callers = 50
	var readLat, writeLat [][]time.Duration
	stop := make(chan struct{})
	var stopped atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < callers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var reads, writes []time.Duration
			for {
				select {
				case <-stop:
					readLat = append(readLat, reads)
					writeLat = append(writeLat, writes)
					return
				default:
				}
				id := fmt.Sprintf("cust-%06d", rng.Intn(n))
				if rng.Intn(2) == 0 {
					start := time.Now()
					if _, err := s.Get(id); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
					reads = append(reads, time.Since(start))
				} else {
					start := time.Now()
					if err := s.Save(id, fmt.Sprintf("91%013d%04x", rng.Intn(n), rng.Int())); err != nil {
						t.Errorf("Save: %v", err)
						return
					}
					writes = append(writes, time.Since(start))
				}
			}
		}(int64(w*7 + 1))
	}

	rotStart := time.Now()
	if err := s.StartRotation("perf-rotation"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 业务读写持续 5 秒后停止写入，模拟维护窗口结束。
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()
	stopped.Store(true)
	stopAt := time.Now()

	deadline := stopAt.Add(60 * time.Second)
	var res RotationResult
	var err error
	for {
		res, err = s.RotationStatus("perf-rotation")
		if err != nil {
			t.Fatalf("RotationStatus: %v", err)
		}
		if res.State == RotationCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotation not completed within 60s after writes stopped: %+v", res)
		}
		time.Sleep(50 * time.Millisecond)
	}
	rotTotal := time.Since(rotStart)
	rotAfterStop := time.Since(stopAt)

	flat := func(src [][]time.Duration) []time.Duration {
		var out []time.Duration
		for _, v := range src {
			out = append(out, v...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	p95 := func(sorted []time.Duration) time.Duration {
		if len(sorted) == 0 {
			return 0
		}
		return sorted[len(sorted)*95/100]
	}
	reads := flat(readLat)
	writes := flat(writeLat)
	readP95, writeP95 := p95(reads), p95(writes)
	t.Logf("reads: %d ops, p95=%v; writes: %d ops, p95=%v", len(reads), readP95, len(writes), writeP95)
	t.Logf("rotation total=%v, after-writes-stopped=%v", rotTotal, rotAfterStop)
	if readP95 > 200*time.Millisecond {
		t.Fatalf("read p95 %v exceeds 200ms", readP95)
	}
	if writeP95 > 200*time.Millisecond {
		t.Fatalf("write p95 %v exceeds 200ms", writeP95)
	}
	if rotAfterStop > 60*time.Second {
		t.Fatalf("rotation took %v after writes stopped, exceeds 60s", rotAfterStop)
	}

	// 轮换后只剩当前密钥，全部客户可读回。
	ids, err := listKeyIDs(keyDir)
	if err != nil || len(ids) != 1 {
		t.Fatalf("keys = %v, %v; want single current key", ids, err)
	}
	for i := 0; i < 200; i++ {
		if _, err := s.Get(fmt.Sprintf("cust-%06d", i*500)); err != nil {
			t.Fatalf("Get after rotation: %v", err)
		}
	}
}
