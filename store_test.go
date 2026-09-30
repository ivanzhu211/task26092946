package taxvault

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// openTemp 在临时目录打开一个 Store。
func openTemp(t *testing.T) (*Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, dbPath, keyDir
}

// waitRotation 等待轮换完成，超时失败。
func waitRotation(t *testing.T, s *Store, taskID string) RotationResult {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		res, err := s.RotationStatus(taskID)
		if err != nil {
			t.Fatalf("RotationStatus(%s): %v", taskID, err)
		}
		if res.State == RotationCompleted {
			return res
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotation %s not completed in time: %+v", taskID, res)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSaveGetRoundtrip(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	if err := s.Save("c1", "91310000MA1K000000"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Get("c1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "91310000MA1K000000" {
		t.Fatalf("got %q", got)
	}
	// 替换
	if err := s.Save("c1", "91110000MA01YYYYYY"); err != nil {
		t.Fatalf("Save replace: %v", err)
	}
	got, err = s.Get("c1")
	if err != nil {
		t.Fatalf("Get after replace: %v", err)
	}
	if got != "91110000MA01YYYYYY" {
		t.Fatalf("after replace got %q", got)
	}
	// 不存在
	if _, err := s.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	s, dbPath, keyDir := openTemp(t)
	want := map[string]string{}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("cust-%04d", i)
		tax := fmt.Sprintf("91%016d", i)
		want[id] = tax
		if err := s.Save(id, tax); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for id, tax := range want {
		got, err := s2.Get(id)
		if err != nil {
			t.Fatalf("Get %s after reopen: %v", id, err)
		}
		if got != tax {
			t.Fatalf("%s: got %q want %q", id, got, tax)
		}
	}
}

func TestRotationBasicAndIdempotent(t *testing.T) {
	s, _, keyDir := openTemp(t)
	defer s.Close()
	for i := 0; i < 50; i++ {
		if err := s.Save(fmt.Sprintf("c%03d", i), fmt.Sprintf("tax-%03d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	keysBefore, err := listKeyIDs(keyDir)
	if err != nil {
		t.Fatalf("listKeyIDs: %v", err)
	}
	if len(keysBefore) != 1 {
		t.Fatalf("initial keys = %d, want 1", len(keysBefore))
	}
	if err := s.StartRotation("task-1"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	// 同一标识重复提交：不生成新一轮。
	if err := s.StartRotation("task-1"); err != nil {
		t.Fatalf("StartRotation same id: %v", err)
	}
	res := waitRotation(t, s, "task-1")
	if res.State != RotationCompleted || res.Remaining != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	// 完成后再次重复提交：不改变当前密钥。
	keysAfter, err := listKeyIDs(keyDir)
	if err != nil {
		t.Fatalf("listKeyIDs after: %v", err)
	}
	if len(keysAfter) != 1 || keysAfter[0] == keysBefore[0] {
		t.Fatalf("keys after = %v, want single new key (old %v)", keysAfter, keysBefore)
	}
	if err := s.StartRotation("task-1"); err != nil {
		t.Fatalf("StartRotation completed id: %v", err)
	}
	keysRetry, _ := listKeyIDs(keyDir)
	if len(keysRetry) != 1 || keysRetry[0] != keysAfter[0] {
		t.Fatalf("retry changed keys: %v -> %v", keysAfter, keysRetry)
	}
	// 全部记录仍可读。
	for i := 0; i < 50; i++ {
		got, err := s.Get(fmt.Sprintf("c%03d", i))
		if err != nil {
			t.Fatalf("Get after rotation: %v", err)
		}
		if want := fmt.Sprintf("tax-%03d", i); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

func TestRotationBusy(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	// 足够多的记录让迁移持续一段时间。
	for i := 0; i < 5000; i++ {
		if err := s.Save(fmt.Sprintf("c%05d", i), fmt.Sprintf("tax-%05d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := s.StartRotation("task-a"); err != nil {
		t.Fatalf("StartRotation: %v", err)
	}
	err := s.StartRotation("task-b")
	if err != nil && !errors.Is(err, ErrRotationBusy) {
		t.Fatalf("want ErrRotationBusy, got %v", err)
	}
	// task-b 可能恰好赶上 task-a 已完成；若已受理则等它完成。
	if err == nil {
		waitRotation(t, s, "task-b")
	} else {
		waitRotation(t, s, "task-a")
	}
}

func TestRotationStatusUnknownTask(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	if _, err := s.RotationStatus("no-such-task"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
}

func TestCryptoRoundtrip(t *testing.T) {
	_, key, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey: %v", err)
	}
	ct, err := encrypt(key, []byte("hello-tax"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	pt, err := decrypt(key, ct)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(pt) != "hello-tax" {
		t.Fatalf("got %q", pt)
	}
	// 错密钥不能解。
	_, other, _ := generateKey()
	if _, err := decrypt(other, ct); err == nil {
		t.Fatal("decrypt with wrong key should fail")
	}
}

func TestKeyStoreFiles(t *testing.T) {
	dir := t.TempDir()
	id, key, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey: %v", err)
	}
	if err := writeKeyFile(dir, id, key); err != nil {
		t.Fatalf("writeKeyFile: %v", err)
	}
	got, err := readKeyFile(dir, id)
	if err != nil {
		t.Fatalf("readKeyFile: %v", err)
	}
	if string(got) != string(key) {
		t.Fatal("key mismatch")
	}
	ids, err := listKeyIDs(dir)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("listKeyIDs = %v, %v", ids, err)
	}
	if err := removeKeyFiles(dir, []string{id}); err != nil {
		t.Fatalf("removeKeyFiles: %v", err)
	}
	if _, err := readKeyFile(dir, id); !errors.Is(err, errKeyMissing) {
		t.Fatalf("want errKeyMissing, got %v", err)
	}
}
