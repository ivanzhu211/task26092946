package taxvault

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// RotationStatus 表示一轮轮换的状态。
type RotationStatus string

const (
	RotationInProgress RotationStatus = "in_progress"
	RotationCompleted  RotationStatus = "completed"
)

// RotationResult 描述一轮轮换任务的结果。
type RotationResult struct {
	TaskID     string
	Status     RotationStatus
	Total      int // 任务受理时的记录总数（进行中为当前总数）
	Migrated   int // 已使用新密钥的记录数
	StartedAt  time.Time
	FinishedAt time.Time // 未完成时为零值
}

const migrateBatchSize = 500

// StartRotation 发起一轮密钥轮换。
//
// taskID 由调用方保证全局唯一：同一标识重复提交时，若原轮换仍在进行则返回
// 当前进度，若已完成则返回原结果，均不会生成新一轮。已受理返回后，后续
// 成功的 Save 均使用新密钥。同一时刻只允许一轮未完成的轮换。
func (s *Store) StartRotation(ctx context.Context, taskID string) (RotationResult, error) {
	if taskID == "" {
		return RotationResult{}, errors.New("taxvault: task id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return RotationResult{}, ErrClosed
	}
	// 同一任务标识：继续原轮换或返回原结果。
	if res, found, err := s.rotationStatusLocked(ctx, taskID); err != nil {
		return RotationResult{}, err
	} else if found {
		return res, nil
	}
	// 不同任务标识但已有进行中的轮换。
	if s.activeTask != "" {
		return RotationResult{}, fmt.Errorf("%w: %s", ErrRotationInProgress, s.activeTask)
	}
	oldKeyID := s.currentKey
	newKey, err := generateKey()
	if err != nil {
		return RotationResult{}, err
	}
	newKeyID, err := generateKeyID()
	if err != nil {
		return RotationResult{}, err
	}
	// 先落密钥文件，再在一个事务里登记轮换并切换当前密钥。
	if err := writeKey(s.keyDir, newKeyID, newKey); err != nil {
		return RotationResult{}, err
	}
	started := nowUnix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		removeKey(s.keyDir, newKeyID)
		return RotationResult{}, fmt.Errorf("begin rotation tx: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO rotations (task_id, old_key_id, new_key_id, status, started_at)
		 VALUES (?,?,?,'active',?)`, taskID, oldKeyID, newKeyID, started); err != nil {
		removeKey(s.keyDir, newKeyID)
		return RotationResult{}, fmt.Errorf("register rotation: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES ('current_key_id', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, newKeyID); err != nil {
		removeKey(s.keyDir, newKeyID)
		return RotationResult{}, fmt.Errorf("switch current key: %w", err)
	}
	if err := tx.Commit(); err != nil {
		removeKey(s.keyDir, newKeyID)
		return RotationResult{}, fmt.Errorf("commit rotation: %w", err)
	}
	s.keys[newKeyID] = newKey
	s.currentKey = newKeyID
	s.activeTask = taskID
	s.startMigratorLocked(taskID, newKeyID)

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tax_records`).Scan(&total); err != nil {
		return RotationResult{}, fmt.Errorf("count records: %w", err)
	}
	return RotationResult{
		TaskID:    taskID,
		Status:    RotationInProgress,
		Total:     total,
		StartedAt: time.Unix(started, 0),
	}, nil
}

// RotationStatus 查询指定任务的轮换结果；found 为 false 表示任务不存在。
func (s *Store) RotationStatus(ctx context.Context, taskID string) (RotationResult, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rotationStatusLocked(ctx, taskID)
}

// rotationStatusLocked 查询任务状态；调用方需持有 s.mu（读或写）。
func (s *Store) rotationStatusLocked(ctx context.Context, taskID string) (RotationResult, bool, error) {
	var (
		status, newKeyID      string
		total, migrated       int
		startedAt, finishedAt int64
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT status, new_key_id, total, migrated, started_at, finished_at
		 FROM rotations WHERE task_id = ?`, taskID).
		Scan(&status, &newKeyID, &total, &migrated, &startedAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RotationResult{}, false, nil
	}
	if err != nil {
		return RotationResult{}, false, fmt.Errorf("query rotation: %w", err)
	}
	res := RotationResult{
		TaskID:    taskID,
		Total:     total,
		Migrated:  migrated,
		StartedAt: time.Unix(startedAt, 0),
	}
	if status == string(RotationCompleted) {
		res.Status = RotationCompleted
		res.FinishedAt = time.Unix(finishedAt, 0)
		return res, true, nil
	}
	res.Status = RotationInProgress
	// 进行中的任务报告实时进度。
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(key_id = ?), 0) FROM tax_records`, newKeyID).
		Scan(&res.Total, &res.Migrated); err != nil {
		return RotationResult{}, false, fmt.Errorf("query rotation progress: %w", err)
	}
	return res, true, nil
}

// recover 打开时的恢复编排：确定当前密钥、恢复未完成的轮换、清理陈旧密钥。
// 调用时 s.keys 已从密钥目录加载。
func (s *Store) recover() error {
	ctx := context.Background()
	var currentMeta string
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM meta WHERE key = 'current_key_id'`).Scan(&currentMeta)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read current key meta: %w", err)
	}
	var (
		taskID, oldKeyID, newKeyID string
	)
	hasActive := true
	err = s.db.QueryRowContext(ctx,
		`SELECT task_id, old_key_id, new_key_id FROM rotations WHERE status = 'active'`).
		Scan(&taskID, &oldKeyID, &newKeyID)
	if errors.Is(err, sql.ErrNoRows) {
		hasActive = false
	} else if err != nil {
		return fmt.Errorf("read active rotation: %w", err)
	}

	var recordCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tax_records`).Scan(&recordCount); err != nil {
		return fmt.Errorf("count records: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case hasActive:
		// 轮换中断：以新密钥为当前密钥继续。
		if _, ok := s.keys[newKeyID]; !ok {
			return fmt.Errorf("taxvault: new key %q of interrupted rotation missing from key dir", newKeyID)
		}
		s.currentKey = newKeyID
		s.activeTask = taskID
		if currentMeta != newKeyID {
			if _, err := s.db.ExecContext(ctx,
				`INSERT INTO meta (key, value) VALUES ('current_key_id', ?)
				 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, newKeyID); err != nil {
				return fmt.Errorf("restore current key meta: %w", err)
			}
		}
	case currentMeta != "":
		if _, ok := s.keys[currentMeta]; !ok {
			return fmt.Errorf("taxvault: current key %q missing from key dir", currentMeta)
		}
		s.currentKey = currentMeta
	case len(s.keys) == 0 && recordCount == 0:
		// 全新库：生成初始密钥。
		key, err := generateKey()
		if err != nil {
			return err
		}
		keyID, err := generateKeyID()
		if err != nil {
			return err
		}
		if err := writeKey(s.keyDir, keyID, key); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO meta (key, value) VALUES ('current_key_id', ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, keyID); err != nil {
			removeKey(s.keyDir, keyID)
			return fmt.Errorf("init current key meta: %w", err)
		}
		s.keys[keyID] = key
		s.currentKey = keyID
	case len(s.keys) == 1 && recordCount >= 0:
		// 元信息缺失但目录中恰有一把密钥：采纳之。
		for id := range s.keys {
			s.currentKey = id
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO meta (key, value) VALUES ('current_key_id', ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, s.currentKey); err != nil {
			return fmt.Errorf("rebuild current key meta: %w", err)
		}
	default:
		return errors.New("taxvault: cannot determine current key")
	}
	if err := s.cleanupStaleKeysLocked(); err != nil {
		return err
	}
	if s.activeTask != "" {
		s.startMigratorLocked(s.activeTask, s.currentKey)
	}
	return nil
}

// startMigratorLocked 启动后台迁移协程；调用方需持有 s.mu 写锁。
func (s *Store) startMigratorLocked(taskID, newKeyID string) {
	s.rotWg.Add(1)
	go func() {
		defer s.rotWg.Done()
		for {
			select {
			case <-s.rotStop:
				return
			default:
			}
			done, err := s.migrateBatch(newKeyID, migrateBatchSize)
			if err != nil {
				select {
				case <-s.rotStop:
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			if done {
				s.finalizeRotation(taskID, newKeyID)
				return
			}
		}
	}()
}

// migrateBatch 迁移至多 limit 条尚未使用 newKeyID 的记录。
// 条件更新保证不覆盖迁移期间并发 Save 写入的新值。返回 done=true 表示无待迁移记录。
func (s *Store) migrateBatch(newKeyID string, limit int) (bool, error) {
	keys, _ := s.snapshotKeys()
	newKey, ok := keys[newKeyID]
	if !ok {
		return false, fmt.Errorf("taxvault: new key %q not loaded", newKeyID)
	}
	rows, err := s.db.Query(
		`SELECT client_id, ciphertext, key_id FROM tax_records WHERE key_id != ? LIMIT ?`,
		newKeyID, limit)
	if err != nil {
		return false, fmt.Errorf("select stale records: %w", err)
	}
	type staleRow struct {
		clientID   string
		ciphertext []byte
		keyID      string
	}
	var batch []staleRow
	for rows.Next() {
		var r staleRow
		if err := rows.Scan(&r.clientID, &r.ciphertext, &r.keyID); err != nil {
			rows.Close()
			return false, fmt.Errorf("scan stale record: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, fmt.Errorf("iterate stale records: %w", err)
	}
	rows.Close()
	if len(batch) == 0 {
		return true, nil
	}
	if s.testHookBatch != nil {
		s.testHookBatch()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, fmt.Errorf("begin migrate tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(
		`UPDATE tax_records SET ciphertext = ?, key_id = ? WHERE client_id = ? AND key_id = ?`)
	if err != nil {
		return false, fmt.Errorf("prepare migrate stmt: %w", err)
	}
	defer stmt.Close()
	for _, r := range batch {
		oldKey, ok := keys[r.keyID]
		if !ok {
			return false, fmt.Errorf("taxvault: key %q of record %q not loaded", r.keyID, r.clientID)
		}
		plain, err := decrypt(oldKey, r.ciphertext)
		if err != nil {
			return false, fmt.Errorf("decrypt record %q: %w", r.clientID, err)
		}
		ciphertext, err := encrypt(newKey, plain)
		if err != nil {
			return false, err
		}
		if _, err := stmt.Exec(ciphertext, newKeyID, r.clientID, r.keyID); err != nil {
			return false, fmt.Errorf("re-encrypt record %q: %w", r.clientID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit migrate tx: %w", err)
	}
	return false, nil
}

// finalizeRotation 收尾一轮轮换：确认全部记录已用新密钥，标记完成并清理旧密钥。
func (s *Store) finalizeRotation(taskID, newKeyID string) {
	var remaining int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM tax_records WHERE key_id != ?`, newKeyID).Scan(&remaining); err != nil {
		return // 下次启动时 recover 会重试
	}
	if remaining > 0 {
		return
	}
	var total int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM tax_records`).Scan(&total); err != nil {
		return
	}
	// 先清理旧密钥，再标记完成：报告完成时旧密钥必须已从密钥目录移除。
	// 若中途崩溃，recover 会重跑迁移与收尾，两步均为幂等。
	s.mu.Lock()
	if err := s.cleanupStaleKeysLocked(); err != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	if _, err := s.db.Exec(
		`UPDATE rotations SET status = 'completed', total = ?, migrated = ?, finished_at = ?
		 WHERE task_id = ? AND status = 'active'`, total, total, nowUnix(), taskID); err != nil {
		return
	}
	s.mu.Lock()
	if s.activeTask == taskID {
		s.activeTask = ""
	}
	s.mu.Unlock()
}

// cleanupStaleKeysLocked 删除不再被任何记录引用、且非当前密钥的密钥文件。
// 调用方需持有 s.mu 写锁。
func (s *Store) cleanupStaleKeysLocked() error {
	rows, err := s.db.Query(`SELECT DISTINCT key_id FROM tax_records`)
	if err != nil {
		return fmt.Errorf("list referenced keys: %w", err)
	}
	referenced := map[string]bool{s.currentKey: true}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan referenced key: %w", err)
		}
		referenced[id] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate referenced keys: %w", err)
	}
	rows.Close()
	for _, id := range sortedKeyIDs(s.keys) {
		if referenced[id] {
			continue
		}
		if err := removeKey(s.keyDir, id); err != nil {
			return err
		}
		delete(s.keys, id)
	}
	return nil
}

// newKeyID 生成文件名安全的密钥标识。
func generateKeyID() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate key id: %w", err)
	}
	return "k" + hex.EncodeToString(buf), nil
}
