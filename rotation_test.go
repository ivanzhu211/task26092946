package taxvault

import (
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// TestMigrationOverlappingWrites 迁移期间持续修改同一批客户，
// 最终值必须等于最后确认保存的值。
func TestMigrationOverlappingWrites(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	const n = 3000
	expected := map[string]string{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%05d", i)
		tax := fmt.Sprintf("tax-v0-%05d", i)
		expected[id] = tax
		if err := s.Save(id, tax); err != nil {
			t.Fatalf("seed Save: %v", err)
		}
	}
	if err := s.StartRotation("rot-overlap"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			ver := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ver++
				id := fmt.Sprintf("c%05d", rng.Intn(n))
				tax := fmt.Sprintf("tax-w%d-v%d", seed, ver)
				if err := s.Save(id, tax); err != nil {
					return // 关闭即退出
				}
				mu.Lock()
				expected[id] = tax
				mu.Unlock()
			}
		}(int64(w + 1))
	}
	waitRotation(t, s, "rot-overlap")
	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for id, want := range expected {
		got, err := s.Get(id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("%s: got %q want %q (最后确认保存的值)", id, got, want)
		}
	}
}

// TestRestartWithMigrationIncomplete 新密钥已启用但迁移未完成时退出，
// 重开后自动继续，逐条读取结果与预期一致。
func TestRestartWithMigrationIncomplete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 20000
	expected := map[string]string{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%06d", i)
		tax := fmt.Sprintf("tax-%06d", i)
		expected[id] = tax
		if err := s.Save(id, tax); err != nil {
			t.Fatalf("seed Save: %v", err)
		}
	}
	if err := s.StartRotation("rot-restart"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 立即关闭，保证退出时迁移远未完成。
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 重开：自动继续轮换，无需人工干预。
	s2, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	// 恢复期间新保存必须使用新密钥，旧记录仍可读可改。
	if err := s2.Save("c000000", "tax-updated-during-recovery"); err != nil {
		t.Fatalf("Save during recovery: %v", err)
	}
	expected["c000000"] = "tax-updated-during-recovery"
	waitRotation(t, s2, "rot-restart")
	for id, want := range expected {
		got, err := s2.Get(id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("%s: got %q want %q", id, got, want)
		}
	}
	ids, err := listKeyIDs(keyDir)
	if err != nil || len(ids) != 1 {
		t.Fatalf("keys after recovery = %v, %v; want single current key", ids, err)
	}
}

// TestRestartWithStaleKeyFile 旧密钥清理尚未结束（文件残留）时退出，
// 恢复后不再需要的密钥必须得到清理。
func TestRestartWithStaleKeyFile(t *testing.T) {
	s, dbPath, keyDir := openTemp(t)
	for i := 0; i < 500; i++ {
		if err := s.Save(fmt.Sprintf("c%04d", i), fmt.Sprintf("tax-%04d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := s.StartRotation("rot-cleanup"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	waitRotation(t, s, "rot-cleanup")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 伪造“清理未结束”：密钥目录里残留一个不再被引用的密钥文件。
	_, staleKey, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey: %v", err)
	}
	if err := writeKeyFile(keyDir, "stale-old-key", staleKey); err != nil {
		t.Fatalf("write stale key: %v", err)
	}
	s2, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	ids, err := listKeyIDs(keyDir)
	if err != nil {
		t.Fatalf("listKeyIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("stale key not cleaned: %v", ids)
	}
	got, err := s2.Get("c0000")
	if err != nil || got != "tax-0000" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

// TestRestartFinalizeNotCommitted 伪造“旧密钥文件已删除、但完成标记未提交”
// 的中间状态，恢复后应幂等收尾，不能增加轮换轮数或丢数据。
func TestRestartFinalizeNotCommitted(t *testing.T) {
	s, dbPath, keyDir := openTemp(t)
	const n = 1000
	for i := 0; i < n; i++ {
		if err := s.Save(fmt.Sprintf("c%05d", i), fmt.Sprintf("tax-%05d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := s.StartRotation("rot-half-done"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	res := waitRotation(t, s, "rot-half-done")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 回滚完成标记：任务回到进行中，meta 指回已删除的旧密钥。
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.Exec(`UPDATE rotation_tasks SET state=?, completed_at=0 WHERE task_id=?`,
		string(RotationInProgress), "rot-half-done"); err != nil {
		t.Fatalf("revert task state: %v", err)
	}
	if _, err := db.Exec(`UPDATE meta SET value='deleted-old-key' WHERE key=?`, metaKeyCurrentKeyID); err != nil {
		t.Fatalf("revert meta: %v", err)
	}
	db.Close()
	s2, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	res2 := waitRotation(t, s2, "rot-half-done")
	if res2.NewKeyID != res.NewKeyID {
		t.Fatalf("recovery generated a new key: %s -> %s", res.NewKeyID, res2.NewKeyID)
	}
	ids, _ := listKeyIDs(keyDir)
	if len(ids) != 1 || ids[0] != res.NewKeyID {
		t.Fatalf("keys = %v, want [%s]", ids, res.NewKeyID)
	}
	for i := 0; i < n; i++ {
		got, err := s2.Get(fmt.Sprintf("c%05d", i))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if want := fmt.Sprintf("tax-%05d", i); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

// TestTwoRotationsAndHistoricalRetry 连续两轮轮换及历史任务重试：
// 最终只剩当前密钥，全部客户仍可读回。
func TestTwoRotationsAndHistoricalRetry(t *testing.T) {
	s, _, keyDir := openTemp(t)
	defer s.Close()
	const n = 2000
	for i := 0; i < n; i++ {
		if err := s.Save(fmt.Sprintf("c%05d", i), fmt.Sprintf("tax-%05d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := s.StartRotation("round-1"); err != nil {
		t.Fatalf("StartRotation round-1: %v", err)
	}
	r1 := waitRotation(t, s, "round-1")
	if err := s.StartRotation("round-2"); err != nil {
		t.Fatalf("StartRotation round-2: %v", err)
	}
	r2 := waitRotation(t, s, "round-2")
	if r1.NewKeyID == r2.NewKeyID {
		t.Fatal("两轮轮换生成了相同密钥")
	}
	// 历史任务重试：返回原结果，不改变当前密钥。
	if err := s.StartRotation("round-1"); err != nil {
		t.Fatalf("retry round-1: %v", err)
	}
	hist, err := s.RotationStatus("round-1")
	if err != nil {
		t.Fatalf("RotationStatus round-1: %v", err)
	}
	if hist.State != RotationCompleted || hist.NewKeyID != r1.NewKeyID {
		t.Fatalf("historical result changed: %+v", hist)
	}
	ids, err := listKeyIDs(keyDir)
	if err != nil {
		t.Fatalf("listKeyIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != r2.NewKeyID {
		t.Fatalf("keys = %v, want only current %s", ids, r2.NewKeyID)
	}
	for i := 0; i < n; i++ {
		got, err := s.Get(fmt.Sprintf("c%05d", i))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if want := fmt.Sprintf("tax-%05d", i); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

// TestRepeatedCrashConvergence 连续多次退出和重启仍应收敛：
// 不增加轮换轮数、不遗漏客户、不永久积累旧密钥。
func TestRepeatedCrashConvergence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	expected := map[string]string{}
	seed := func(s *Store, from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			id := fmt.Sprintf("c%05d", i)
			tax := fmt.Sprintf("tax-%05d", i)
			expected[id] = tax
			if err := s.Save(id, tax); err != nil {
				t.Fatalf("Save: %v", err)
			}
		}
	}
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	seed(s, 0, 3000)
	if err := s.StartRotation("rot-crash"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	s.Close()
	// 连续多次“打开-写一点-立刻退出”，轮换应在反复中断后仍收敛。
	for round := 0; round < 5; round++ {
		s, err := Open(dbPath, keyDir)
		if err != nil {
			t.Fatalf("reopen round %d: %v", round, err)
		}
		// 重复提交同一任务标识：只能继续原轮换。
		if err := s.StartRotation("rot-crash"); err != nil {
			t.Fatalf("re-submit round %d: %v", round, err)
		}
		base := 3000 + round*10
		seed(s, base, base+10)
		s.Close()
	}
	s, err = Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer s.Close()
	res := waitRotation(t, s, "rot-crash")
	if res.State != RotationCompleted {
		t.Fatalf("not completed: %+v", res)
	}
	ids, _ := listKeyIDs(keyDir)
	if len(ids) != 1 {
		t.Fatalf("old keys accumulated: %v", ids)
	}
	for id, want := range expected {
		got, err := s.Get(id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("%s: got %q want %q", id, got, want)
		}
	}
	// 任务表中没有因为重复提交产生额外轮换。
	var taskCount int
	raw, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer raw.Close()
	if err := raw.QueryRow(`SELECT COUNT(*) FROM rotation_tasks`).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("rotation tasks = %d, want 1", taskCount)
	}
}
