package taxvault

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestSaveAndGet(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	ctx := context.Background()

	if _, found, err := s.Get(ctx, "c1"); err != nil || found {
		t.Fatalf("Get missing: found=%v err=%v", found, err)
	}
	if err := s.Save(ctx, "c1", "91310000MA1K000001"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, found, err := s.Get(ctx, "c1")
	if err != nil || !found || got != "91310000MA1K000001" {
		t.Fatalf("Get: got=%q found=%v err=%v", got, found, err)
	}
	// 替换
	if err := s.Save(ctx, "c1", "91310000MA1K000002"); err != nil {
		t.Fatalf("Save replace: %v", err)
	}
	got, _, _ = s.Get(ctx, "c1")
	if got != "91310000MA1K000002" {
		t.Fatalf("after replace: got=%q", got)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	s, dbPath, keyDir := openTemp(t)
	ctx := context.Background()
	for _, tc := range [][2]string{{"a", "TAX-A"}, {"b", "TAX-B"}, {"c", "TAX-C"}} {
		if err := s.Save(ctx, tc[0], tc[1]); err != nil {
			t.Fatalf("Save: %v", err)
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
	for _, tc := range [][2]string{{"a", "TAX-A"}, {"b", "TAX-B"}, {"c", "TAX-C"}} {
		got, found, err := s2.Get(ctx, tc[0])
		if err != nil || !found || got != tc[1] {
			t.Fatalf("Get %s: got=%q found=%v err=%v", tc[0], got, found, err)
		}
	}
}

func TestNoPlaintextInDB(t *testing.T) {
	s, dbPath, keyDir := openTemp(t)
	ctx := context.Background()
	secret := "91310000SECRETTAX99"
	if err := s.Save(ctx, "c1", secret); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 数据库文件（含 WAL）中不得出现明文税号。
	for _, f := range []string{dbPath, dbPath + "-wal"} {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if strings.Contains(string(data), secret) {
			t.Fatalf("plaintext tax id found in %s", f)
		}
	}
	// 密钥目录中不得出现明文税号。
	entries, _ := os.ReadDir(keyDir)
	for _, e := range entries {
		data, _ := os.ReadFile(filepath.Join(keyDir, e.Name()))
		if strings.Contains(string(data), secret) {
			t.Fatalf("plaintext tax id found in key file %s", e.Name())
		}
	}
}

func TestSaveEmptyClientID(t *testing.T) {
	s, _, _ := openTemp(t)
	defer s.Close()
	if err := s.Save(context.Background(), "", "X"); err == nil {
		t.Fatal("expected error for empty client id")
	}
}
