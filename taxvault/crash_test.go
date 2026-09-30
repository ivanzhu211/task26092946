package taxvault

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestHelperProcess 不是测试：作为子进程运行，模拟“进程在轮换任意阶段退出”。
// 通过环境变量控制行为，退出时不调用 Close（os.Exit），模拟办公软件被直接关闭。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("TAXVAULT_HELPER") == "" {
		return
	}
	dbPath := os.Getenv("TAXVAULT_DB")
	keyDir := os.Getenv("TAXVAULT_KEYS")
	mode := os.Getenv("TAXVAULT_MODE")
	ctx := context.Background()

	s, err := Open(dbPath, keyDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper open:", err)
		os.Exit(3)
	}
	switch mode {
	case "seed-and-crash-mid-migration":
		n, _ := strconv.Atoi(os.Getenv("TAXVAULT_N"))
		for i := 0; i < n; i++ {
			if err := s.Save(ctx, fmt.Sprintf("c%06d", i), fmt.Sprintf("INIT-%d", i)); err != nil {
				fmt.Fprintln(os.Stderr, "helper seed:", err)
				os.Exit(3)
			}
		}
		if _, err := s.StartRotation(ctx, "rot-crash"); err != nil {
			fmt.Fprintln(os.Stderr, "helper rotate:", err)
			os.Exit(3)
		}
		// 等迁移真正开始（新密钥已启用、部分记录已迁移）后立刻退出。
		for {
			res, found, err := s.RotationStatus(ctx, "rot-crash")
			if err == nil && found && res.Migrated > 0 && res.Migrated < res.Total {
				break
			}
			time.Sleep(time.Millisecond)
		}
		os.Exit(2) // 不 Close，模拟进程被杀
	case "workload-crash":
		// 轮换期间持续写入，每条已确认的保存都记入 expected.log，随后随机退出。
		n, _ := strconv.Atoi(os.Getenv("TAXVAULT_N"))
		logPath := os.Getenv("TAXVAULT_EXPECTED")
		first := os.Getenv("TAXVAULT_FIRST") == "1"
		if first {
			for i := 0; i < n; i++ {
				if err := s.Save(ctx, fmt.Sprintf("c%06d", i), fmt.Sprintf("INIT-%d", i)); err != nil {
					fmt.Fprintln(os.Stderr, "helper seed:", err)
					os.Exit(3)
				}
			}
			if _, err := s.StartRotation(ctx, "rot-crash"); err != nil {
				fmt.Fprintln(os.Stderr, "helper rotate:", err)
				os.Exit(3)
			}
		}
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper log:", err)
			os.Exit(3)
		}
		rounds, _ := strconv.Atoi(os.Getenv("TAXVAULT_ROUNDS"))
		for i := 0; i < rounds; i++ {
			id := fmt.Sprintf("c%06d", (i*7919)%n)
			val := fmt.Sprintf("W-%d-%d", os.Getpid(), i)
			if err := s.Save(ctx, id, val); err != nil {
				continue // 进程随时退出，未确认的保存不算数
			}
			fmt.Fprintf(f, "%s %s\n", id, val)
			f.Sync()
		}
		f.Close()
		os.Exit(2)
	}
	os.Exit(0)
}

// runHelper 以子进程方式运行 TestHelperProcess。
func runHelper(t *testing.T, env map[string]string) {
	t.Helper()
	args := []string{"-test.run=^TestHelperProcess$", "-test.v"}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "TAXVAULT_HELPER=1")
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() == 2 {
			return // 预期的“崩溃”
		}
	}
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, out)
	}
}

func TestCrashMidMigrationRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	const n = 20000

	runHelper(t, map[string]string{
		"TAXVAULT_DB": dbPath, "TAXVAULT_KEYS": keyDir,
		"TAXVAULT_MODE": "seed-and-crash-mid-migration", "TAXVAULT_N": strconv.Itoa(n),
	})
	// 崩溃时：新密钥已启用但迁移未完成，密钥目录里应有两把密钥。
	if got := len(keyFiles(t, keyDir)); got != 2 {
		t.Fatalf("keys at crash = %d, want 2", got)
	}
	// 重新打开：自动继续轮换，无需人工干预。
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	final := waitRotation(t, s, "rot-crash")
	if final.Status != RotationCompleted || final.Migrated != n {
		t.Fatalf("final: %+v", final)
	}
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys after recovery = %d, want 1", got)
	}
	ctx := context.Background()
	for i := 0; i < n; i += 97 { // 抽样逐条校验
		got, found, err := s.Get(ctx, fmt.Sprintf("c%06d", i))
		if err != nil || !found || got != fmt.Sprintf("INIT-%d", i) {
			t.Fatalf("Get c%06d: got=%q found=%v err=%v", i, got, found, err)
		}
	}
}

func TestRepeatedCrashConvergence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	const n = 15000

	env := map[string]string{
		"TAXVAULT_DB": dbPath, "TAXVAULT_KEYS": keyDir,
		"TAXVAULT_MODE": "seed-and-crash-mid-migration", "TAXVAULT_N": strconv.Itoa(n),
	}
	runHelper(t, env) // 首轮：种子 + 崩溃
	// 连续多次崩溃：每次都在迁移中途退出。
	for i := 0; i < 3; i++ {
		runHelper(t, map[string]string{
			"TAXVAULT_DB": dbPath, "TAXVAULT_KEYS": keyDir,
			"TAXVAULT_MODE": "seed-and-crash-mid-migration", "TAXVAULT_N": "0",
		})
	}
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	waitRotation(t, s, "rot-crash")
	// 重复处理不得增加轮换轮数。
	var rotCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM rotations`).Scan(&rotCount); err != nil {
		t.Fatalf("count rotations: %v", err)
	}
	if rotCount != 1 {
		t.Fatalf("rotations = %d, want 1", rotCount)
	}
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys = %d, want 1 (no stale key accumulation)", got)
	}
	var badKeyRows int
	s.db.QueryRow(`SELECT COUNT(*) FROM tax_records WHERE key_id != (SELECT value FROM meta WHERE key='current_key_id')`).Scan(&badKeyRows)
	if badKeyRows != 0 {
		t.Fatalf("rows not on current key: %d", badKeyRows)
	}
}

func TestCrashDuringWorkloadThenVerify(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	logPath := filepath.Join(dir, "expected.log")
	const n = 5000

	// 第一次：种子 + 发起轮换 + 写一部分后崩溃。
	runHelper(t, map[string]string{
		"TAXVAULT_DB": dbPath, "TAXVAULT_KEYS": keyDir,
		"TAXVAULT_MODE": "workload-crash", "TAXVAULT_N": strconv.Itoa(n),
		"TAXVAULT_EXPECTED": logPath, "TAXVAULT_FIRST": "1", "TAXVAULT_ROUNDS": "800",
	})
	// 第二次：恢复后继续写一部分再崩溃（覆盖清理阶段前后的退出）。
	runHelper(t, map[string]string{
		"TAXVAULT_DB": dbPath, "TAXVAULT_KEYS": keyDir,
		"TAXVAULT_MODE": "workload-crash", "TAXVAULT_N": strconv.Itoa(n),
		"TAXVAULT_EXPECTED": logPath, "TAXVAULT_ROUNDS": "800",
	})
	// 独立维护的预期值：每个客户最后一次确认的保存。
	expected := make(map[string]string, n)
	for i := 0; i < n; i++ {
		expected[fmt.Sprintf("c%06d", i)] = fmt.Sprintf("INIT-%d", i)
	}
	if f, err := os.Open(logPath); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			parts := strings.Fields(sc.Text())
			if len(parts) == 2 {
				expected[parts[0]] = parts[1]
			}
		}
		f.Close()
	}
	// 恢复并等待轮换收敛。
	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	waitRotation(t, s, "rot-crash")
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys after recovery = %d, want 1", got)
	}
	ctx := context.Background()
	for id, want := range expected {
		got, found, err := s.Get(ctx, id)
		if err != nil || !found || got != want {
			t.Fatalf("Get %s: got=%q found=%v err=%v, want %q", id, got, found, err, want)
		}
	}
}

// TestRecoveryWithCleanupPending 手工构造“全部记录已用新密钥、但旧密钥尚未清理”
// 的崩溃现场，验证恢复后旧密钥被移除且全部记录可读。
func TestRecoveryWithCleanupPending(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault.db")
	keyDir := filepath.Join(dir, "keys")
	ctx := context.Background()

	s, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		if err := s.Save(ctx, fmt.Sprintf("c%04d", i), fmt.Sprintf("V-%d", i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	oldKeyID := s.currentKey
	// 手工推进到“新密钥已启用、记录已全部重加密、轮换仍 active、旧密钥文件仍在”。
	newKey, _ := generateKey()
	newKID, _ := generateKeyID()
	if err := writeKey(keyDir, newKID, newKey); err != nil {
		t.Fatalf("writeKey: %v", err)
	}
	rows, err := s.db.Query(`SELECT client_id, ciphertext FROM tax_records`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	type rec struct {
		id string
		ct []byte
	}
	var recs []rec
	for rows.Next() {
		var r rec
		rows.Scan(&r.id, &r.ct)
		recs = append(recs, r)
	}
	rows.Close()
	for _, r := range recs {
		plain, err := decrypt(s.keys[oldKeyID], r.ct)
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		ct, _ := encrypt(newKey, plain)
		if _, err := s.db.Exec(`UPDATE tax_records SET ciphertext=?, key_id=? WHERE client_id=?`, ct, newKID, r.id); err != nil {
			t.Fatalf("re-encrypt: %v", err)
		}
	}
	if _, err := s.db.Exec(
		`INSERT INTO rotations (task_id, old_key_id, new_key_id, status, started_at) VALUES ('rot-x', ?, ?, 'active', ?)`,
		oldKeyID, newKID, nowUnix()); err != nil {
		t.Fatalf("insert rotation: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE meta SET value=? WHERE key='current_key_id'`, newKID); err != nil {
		t.Fatalf("update meta: %v", err)
	}
	s.Close()
	if got := len(keyFiles(t, keyDir)); got != 2 {
		t.Fatalf("keys at crash = %d, want 2", got)
	}
	// 重新打开：应自动完成收尾并清理旧密钥。
	s2, err := Open(dbPath, keyDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	waitRotation(t, s2, "rot-x")
	if got := len(keyFiles(t, keyDir)); got != 1 {
		t.Fatalf("keys after recovery = %d, want 1", got)
	}
	for i := 0; i < n; i++ {
		got, found, err := s2.Get(ctx, fmt.Sprintf("c%04d", i))
		if err != nil || !found || got != fmt.Sprintf("V-%d", i) {
			t.Fatalf("Get c%04d: got=%q found=%v err=%v", i, got, found, err)
		}
	}
}
