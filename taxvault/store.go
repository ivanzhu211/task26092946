// Package taxvault 提供税号加密存储能力：税号密文存 SQLite，密钥存独立本地目录。
//
// 四类能力：Save 保存税号、Get 读取税号、StartRotation 发起密钥轮换、
// RotationStatus 查看轮换结果。进程退出后重新 Open 同一数据库与密钥目录，
// 会自动继续未完成的轮换并清理不再需要的密钥。
package taxvault

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrClosed 表示存储库已关闭。
var ErrClosed = errors.New("taxvault: store is closed")

// ErrRotationInProgress 表示已存在另一轮未完成的轮换。
var ErrRotationInProgress = errors.New("taxvault: another rotation is in progress")

const schema = `
CREATE TABLE IF NOT EXISTS tax_records (
	client_id  TEXT PRIMARY KEY,
	ciphertext BLOB NOT NULL,
	key_id     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rotations (
	task_id      TEXT PRIMARY KEY,
	old_key_id   TEXT NOT NULL,
	new_key_id   TEXT NOT NULL,
	status       TEXT NOT NULL CHECK (status IN ('active','completed')),
	total        INTEGER NOT NULL DEFAULT 0,
	migrated     INTEGER NOT NULL DEFAULT 0,
	started_at   INTEGER NOT NULL,
	finished_at  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

// Store 是税号加密存储库的主入口。并发安全。
type Store struct {
	db     *sql.DB
	keyDir string

	mu         sync.RWMutex
	keys       map[string][]byte // keyID -> 密钥
	currentKey string
	activeTask string // 进行中的轮换任务标识，空串表示无
	closed     bool

	rotStop   chan struct{} // 关闭以停止后台迁移
	rotWg     sync.WaitGroup
	rotWakeUp chan struct{} // 通知后台迁移有活要干

	testHookBatch func() // 仅测试用：每批迁移选中记录后回调
}

// Open 打开（必要时创建）数据库与密钥目录，并自动恢复未完成的轮换。
func Open(dbPath, keyDir string) (*Store, error) {
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, fmt.Errorf("create key dir: %w", err)
	}
	if dir := filepath.Dir(dbPath); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)&_pragma=synchronous(normal)",
		filepath.Clean(dbPath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	keys, err := loadKeys(keyDir)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{
		db:        db,
		keyDir:    keyDir,
		keys:      keys,
		rotStop:   make(chan struct{}),
		rotWakeUp: make(chan struct{}, 1),
	}
	if err := s.recover(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close 关闭存储库，等待后台迁移退出。重复调用安全。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.rotStop)
	s.mu.Unlock()
	s.rotWg.Wait()
	return s.db.Close()
}

// Save 新增或替换指定客户的税号。轮换进行中时使用新密钥加密。
func (s *Store) Save(ctx context.Context, clientID, taxID string) error {
	if clientID == "" {
		return errors.New("taxvault: client id is empty")
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	keyID := s.currentKey
	key := s.keys[keyID]
	s.mu.RUnlock()

	ciphertext, err := encrypt(key, []byte(taxID))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tax_records (client_id, ciphertext, key_id) VALUES (?,?,?)
		 ON CONFLICT(client_id) DO UPDATE SET ciphertext=excluded.ciphertext, key_id=excluded.key_id`,
		clientID, ciphertext, keyID)
	if err != nil {
		return fmt.Errorf("save tax id: %w", err)
	}
	return nil
}

// Get 读取指定客户的税号；found 为 false 表示不存在。
func (s *Store) Get(ctx context.Context, clientID string) (taxID string, found bool, err error) {
	var ciphertext []byte
	var keyID string
	err = s.db.QueryRowContext(ctx,
		`SELECT ciphertext, key_id FROM tax_records WHERE client_id = ?`, clientID).
		Scan(&ciphertext, &keyID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get tax id: %w", err)
	}
	s.mu.RLock()
	key, ok := s.keys[keyID]
	s.mu.RUnlock()
	if !ok {
		return "", false, fmt.Errorf("taxvault: key %q not found in key dir", keyID)
	}
	plain, err := decrypt(key, ciphertext)
	if err != nil {
		return "", false, err
	}
	return string(plain), true, nil
}

// snapshotKeys 返回当前密钥映射的副本与当前 keyID。
func (s *Store) snapshotKeys() (map[string][]byte, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string][]byte, len(s.keys))
	for id, k := range s.keys {
		cp[id] = k
	}
	return cp, s.currentKey
}

func nowUnix() int64 { return time.Now().Unix() }
