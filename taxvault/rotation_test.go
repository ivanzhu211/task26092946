package taxvault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func waitRotation(t *testing.T, s *Store, taskID string) RotationResult {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		res, found, err := s.RotationStatus(context.Background(), taskID)
		if err != nil {
			t.Fatalf("RotationStatus: %v", err)
		}
		if !found {
			t.Fatalf("rotation %s not found", taskID)
		}
		if res.Status == RotationCompleted {
			return res
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("rotation %s did not complete in time", taskID)
	return RotationResult{}
}

func keyFiles(t *testing.T, keyDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(keyDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestRotationCompletesAndRemovesOldKey(t *testing.T) {
	s, _, keyDir := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	const n = 200
	for i := 0; i < n; i++ {
		if err := s.Save(ctx, fmt.Sprintf("c%04d", i), fmt.Sprintf("TAX-%d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys before rotation = %d, want 1", got)
	}
	res, err := s.StartRotation(ctx, "rot-1")
	if err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	if res.Status != RotationInProgress || res.Total != n {
		t.Fatalf("unexpected result: %+v", res)
	}
	final := waitRotation(t, s, "rot-1")
	if final.Migrated != n || final.Total != n {
		t.Fatalf("final result: %+v", final)
	}
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys after rotation = %d, want 1: %v", got, keyFiles(t, keyDir))
	}
	for i := 0; i < n; i++ {
		got, found, err := s.Get(ctx, fmt.Sprintf("c%04d", i))
		if err != nil || !found || got != fmt.Sprintf("TAX-%d", i) {
			t.Fatalf("Get c%04d: got=%q found=%v err=%v", i, got, found, err)
		}
	}
}

func TestRotationIdempotentTaskID(t *testing.T) {
	s, _, keyDir := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		s.Save(ctx, fmt.Sprintf("c%d", i), fmt.Sprintf("T%d", i))
	}
	if _, err := s.StartRotation(ctx, "rot-1"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 同一标识重复提交：不生成新密钥。
	if _, err := s.StartRotation(ctx, "rot-1"); err != nil {
		t.Fatalf("resubmit in-progress: %v", err)
	}
	waitRotation(t, s, "rot-1")
	keysAfterFirst := keyFiles(t, keyDir)
	res2, err := s.StartRotation(ctx, "rot-1")
	if err != nil {
		t.Fatalf("resubmit completed: %v", err)
	}
	if res2.Status != RotationCompleted {
		t.Fatalf("resubmit completed: status=%v", res2.Status)
	}
	if got := keyFiles(t, keyDir); len(got) != len(keysAfterFirst) {
		t.Fatalf("resubmit generated new key: %v -> %v", keysAfterFirst, got)
	}
	// 不同标识但已有轮换进行中：拒绝。
	if _, err := s.StartRotation(ctx, "rot-2"); err != nil {
		t.Fatalf("second rotation after completion: %v", err)
	}
	waitRotation(t, s, "rot-2")
	// 已完成的历史任务在后续轮换完成后重试：返回原结果，不改变当前密钥。
	keysAfterSecond := keyFiles(t, keyDir)
	res1again, err := s.StartRotation(ctx, "rot-1")
	if err != nil {
		t.Fatalf("retry historical task: %v", err)
	}
	if res1again.Status != RotationCompleted {
		t.Fatalf("historical retry: status=%v", res1again.Status)
	}
	if got := keyFiles(t, keyDir); len(got) != 1 || len(keysAfterSecond) != 1 {
		t.Fatalf("key files: after2=%v final=%v", keysAfterSecond, got)
	}
	for i := 0; i < 50; i++ {
		got, found, err := s.Get(ctx, fmt.Sprintf("c%d", i))
		if err != nil || !found || got != fmt.Sprintf("T%d", i) {
			t.Fatalf("Get c%d: got=%q found=%v err=%v", i, got, found, err)
		}
	}
}

func TestRotationConflict(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	// 造一批记录并阻塞迁移，保证轮换处于进行中。
	for i := 0; i < 2000; i++ {
		s.Save(ctx, fmt.Sprintf("c%05d", i), fmt.Sprintf("TAX-%d", i))
	}
	block := make(chan struct{})
	s.testHookBatch = func() { <-block; s.testHookBatch = nil }
	if _, err := s.StartRotation(ctx, "rot-a"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	_, err := s.StartRotation(ctx, "rot-b")
	if !errors.Is(err, ErrRotationInProgress) {
		t.Fatalf("want ErrRotationInProgress, got %v", err)
	}
	close(block)
	waitRotation(t, s, "rot-a")
}

func TestMigrationDoesNotClobberConcurrentSave(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	const n = 3000
	for i := 0; i < n; i++ {
		s.Save(ctx, fmt.Sprintf("c%05d", i), fmt.Sprintf("OLD-%d", i))
	}
	// 迁移每批前暂停，测试线程趁机改写同一批里的客户。
	gate := make(chan struct{})
	proceed := make(chan struct{})
	s.testHookBatch = func() {
		select {
		case <-gate:
		default:
			close(gate)
			<-proceed
		}
	}
	if _, err := s.StartRotation(ctx, "rot-1"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	<-gate // 迁移已在第一批前停下
	const victim = "c00000"
	if err := s.Save(ctx, victim, "NEW-VALUE"); err != nil {
		t.Fatalf("Save during rotation: %v", err)
	}
	close(proceed)
	waitRotation(t, s, "rot-1")
	got, found, err := s.Get(ctx, victim)
	if err != nil || !found || got != "NEW-VALUE" {
		t.Fatalf("victim: got=%q found=%v err=%v", got, found, err)
	}
}

func TestReadWriteDuringRotation(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	ctx := context.Background()
	const n = 5000
	for i := 0; i < n; i++ {
		s.Save(ctx, fmt.Sprintf("c%05d", i), fmt.Sprintf("V0-%d", i))
	}
	if _, err := s.StartRotation(ctx, "rot-1"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 轮换期间持续读写，任何客户不得短暂消失。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 64)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("c%05d", (w*1000+i)%n)
				if i%2 == 0 {
					if _, found, err := s.Get(ctx, id); err != nil || !found {
						errCh <- fmt.Errorf("Get %s: found=%v err=%v", id, found, err)
						return
					}
				} else if err := s.Save(ctx, id, fmt.Sprintf("W%d-%d", w, i)); err != nil {
					errCh <- fmt.Errorf("Save %s: %v", id, err)
					return
				}
			}
		}(w)
	}
	waitRotation(t, s, "rot-1")
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}
