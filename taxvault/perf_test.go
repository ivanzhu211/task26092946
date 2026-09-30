package taxvault

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestPerfRotationUnderLoad 验收规模：10 万条客户税号，轮换期间 50 个调用方
// 持续读写，校验读写 P95 < 200ms，停止写入后轮换 60 秒内完成。
func TestPerfRotationUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in -short mode")
	}
	if raceEnabled {
		t.Skip("skipping perf test under -race (timing thresholds not meaningful)")
	}
	s, _, keyDir := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	const n = 100000

	// 快速种子：单事务批量写入密文。
	seedStart := time.Now()
	keys, cur := s.snapshotKeys()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO tax_records (client_id, ciphertext, key_id) VALUES (?,?,?)`)
	if err != nil {
		t.Fatalf("prepare seed: %v", err)
	}
	for i := 0; i < n; i++ {
		ct, err := encrypt(keys[cur], []byte(fmt.Sprintf("INIT-%d", i)))
		if err != nil {
			t.Fatalf("encrypt seed: %v", err)
		}
		if _, err := stmt.Exec(fmt.Sprintf("c%06d", i), ct, cur); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	t.Logf("seeded %d records in %v", n, time.Since(seedStart))

	// 预期值表：仅记录已确认成功的保存。
	var expMu sync.Mutex
	expected := make(map[string]string, n)
	for i := 0; i < n; i++ {
		expected[fmt.Sprintf("c%06d", i)] = fmt.Sprintf("INIT-%d", i)
	}

	const workers = 50
	var latMu sync.Mutex
	var readLat, writeLat []time.Duration
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var localRead, localWrite []time.Duration
			for i := 0; ; i++ {
				select {
				case <-stop:
					latMu.Lock()
					readLat = append(readLat, localRead...)
					writeLat = append(writeLat, localWrite...)
					latMu.Unlock()
					return
				default:
				}
				id := fmt.Sprintf("c%06d", (w*20031+i*7919)%n)
				if w%2 == 0 { // 25 个写方
					val := fmt.Sprintf("W%d-%d", w, i)
					start := time.Now()
					err := s.Save(ctx, id, val)
					el := time.Since(start)
					if err == nil {
						expMu.Lock()
						expected[id] = val
						expMu.Unlock()
						localWrite = append(localWrite, el)
					}
				} else { // 25 个读方
					start := time.Now()
					_, found, err := s.Get(ctx, id)
					el := time.Since(start)
					if err != nil || !found {
						t.Errorf("Get %s: found=%v err=%v", id, found, err)
						return
					}
					localRead = append(localRead, el)
				}
			}
		}(w)
	}

	// 负载运行中发起轮换。
	rotStart := time.Now()
	if _, err := s.StartRotation(ctx, "rot-perf"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 让负载与迁移重叠一段时间。
	time.Sleep(2 * time.Second)
	close(stop) // 停止业务写入
	stoppedAt := time.Now()
	wg.Wait()

	deadline := time.Now().Add(60 * time.Second)
	var final RotationResult
	for time.Now().Before(deadline) {
		res, _, err := s.RotationStatus(ctx, "rot-perf")
		if err != nil {
			t.Fatalf("RotationStatus: %v", err)
		}
		if res.Status == RotationCompleted {
			final = res
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	rotDone := time.Now()
	if final.Status != RotationCompleted {
		t.Fatalf("rotation did not complete within 60s after writes stopped")
	}
	t.Logf("rotation total: %v; after writes stopped: %v; migrated=%d",
		rotDone.Sub(rotStart), rotDone.Sub(stoppedAt), final.Migrated)
	if rotDone.Sub(stoppedAt) > 60*time.Second {
		t.Fatalf("rotation took %v after writes stopped, want < 60s", rotDone.Sub(stoppedAt))
	}

	p95 := func(lat []time.Duration) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		return lat[int(float64(len(lat))*0.95)]
	}
	r95, w95 := p95(readLat), p95(writeLat)
	t.Logf("ops: reads=%d writes=%d; P95 read=%v write=%v", len(readLat), len(writeLat), r95, w95)
	if r95 > 200*time.Millisecond {
		t.Fatalf("read P95 = %v, want < 200ms", r95)
	}
	if w95 > 200*time.Millisecond {
		t.Fatalf("write P95 = %v, want < 200ms", w95)
	}

	// 只剩当前密钥，且全部客户可读回、值等于最后确认的保存。
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys after rotation = %d, want 1", got)
	}
	expMu.Lock()
	defer expMu.Unlock()
	checked := 0
	for id, want := range expected {
		got, found, err := s.Get(ctx, id)
		if err != nil || !found || got != want {
			t.Fatalf("Get %s: got=%q found=%v err=%v, want %q", id, got, found, err, want)
		}
		checked++
	}
	t.Logf("verified %d records against expected values", checked)
}
