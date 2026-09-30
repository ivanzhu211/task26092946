// Package taxvault 提供税号加密存储能力：税号密文存 SQLite，密钥存独立本地目录，
// 支持保存/读取税号、发起密钥轮换、查看轮换结果。
package taxvault

import (
	"database/sql"
	"errors"
	"sync"
)

// ErrNotFound 表示客户标识不存在对应税号。
var ErrNotFound = errors.New("taxvault: customer not found")

// ErrRotationBusy 表示已有未完成的轮换，同一时刻只允许一轮。
var ErrRotationBusy = errors.New("taxvault: another rotation is in progress")

// ErrClosed 表示库已关闭。
var ErrClosed = errors.New("taxvault: store is closed")

// RotationState 描述一轮轮换的生命周期状态。
type RotationState string

const (
	RotationInProgress RotationState = "in_progress"
	RotationCompleted  RotationState = "completed"
)

// RotationResult 是查看轮换结果时返回的信息。
// Migrated/Remaining 反映当前记录在该任务新旧密钥上的实时分布。
type RotationResult struct {
	TaskID      string
	State       RotationState
	NewKeyID    string
	Migrated    int64 // 当前使用该任务新密钥的记录数
	Remaining   int64 // 当前仍未使用该任务新密钥的记录数
	CompletedAt int64 // 完成时间（Unix 秒），未完成时为 0
}

// Store 是税号加密存储库句柄。单进程内并发安全。
//
// 锁约定：Save 持 RLock 完成取钥、加密与落库，轮换收尾持 Lock 复查并切换，
// 二者串行保证“报告完成”与“全部记录已在新密钥下”始终一致。
type Store struct {
	db     *sql.DB
	keyDir string

	mu         sync.RWMutex
	closed     bool
	keys       map[string][]byte // keyID -> 已加载的密钥（内容不可变）
	writeKeyID string            // 当前写入使用的密钥
	rot        *rotationState    // 进行中的轮换，无则 nil
	doneCh     chan struct{}     // 后台迁移停止信号
	wg         sync.WaitGroup
}

// Open 打开（必要时创建）数据库与密钥目录，并自动恢复未完成的轮换。
func Open(dbPath, keyDir string) (*Store, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:     db,
		keyDir: keyDir,
		keys:   map[string][]byte{},
		doneCh: make(chan struct{}),
	}
	if err := s.recoverLocked(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close 停止后台迁移并关闭数据库。已确认的写入不受影响。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.doneCh)
	s.wg.Wait()
	return s.db.Close()
}

// Save 新增或替换指定客户的税号。已确认成功的保存在重新打开库后仍然有效。
func (s *Store) Save(customerID, taxID string) error {
	if customerID == "" {
		return errors.New("taxvault: customer id must not be empty")
	}
	// 持 RLock 覆盖取钥、加密与落库，避免与轮换收尾交错产生旧密钥孤儿记录。
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	key, ok := s.keys[s.writeKeyID]
	if !ok {
		return errKeyMissing
	}
	ciphertext, err := encrypt(key, []byte(taxID))
	if err != nil {
		return err
	}
	return upsertRecord(s.db, customerID, ciphertext, s.writeKeyID)
}

// Get 读取指定客户的完整税号。
func (s *Store) Get(customerID string) (string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return "", ErrClosed
	}
	s.mu.RUnlock()
	rec, err := getRecord(s.db, customerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	s.mu.RLock()
	key, ok := s.keys[rec.KeyID]
	s.mu.RUnlock()
	if !ok {
		return "", errKeyMissing
	}
	plain, err := decrypt(key, rec.Ciphertext)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// StartRotation 发起一轮密钥轮换。taskID 由调用方提供且唯一：
// 相同标识重复提交只继续原轮换或返回原结果，不会生成新一轮。
// 返回已受理后，后续成功保存的税号立即使用新密钥。
func (s *Store) StartRotation(taskID string) error {
	return s.startRotation(taskID)
}

// RotationStatus 查看指定任务标识的轮换结果。
func (s *Store) RotationStatus(taskID string) (RotationResult, error) {
	return s.rotationStatus(taskID)
}
