package taxvault

import (
	"database/sql"
	"errors"
	"time"
)

// ErrTaskNotFound 表示轮换任务标识不存在。
var ErrTaskNotFound = errors.New("taxvault: rotation task not found")

// migrateBatchSize 是每批迁移的记录数，保持事务短小以免阻塞业务读写。
const migrateBatchSize = 200

// rotationState 跟踪进行中轮换的运行时状态。
type rotationState struct {
	taskID   string
	newKeyID string
	oldKeyID string
}

// startRotation 实现 StartRotation。
//
// 幂等规则：taskID 已存在时，进行中则继续原轮换、已完成则直接返回原结果，
// 均不会生成新一轮；仅当无任何未完成轮换时才接受新任务。
func (s *Store) startRotation(taskID string) error {
	if taskID == "" {
		return errors.New("taxvault: task id must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	existing, err := getRotationTask(s.db, taskID)
	if err == nil {
		// 同一标识重复提交：继续原轮换或返回原结果。
		_ = existing
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.rot != nil {
		return ErrRotationBusy
	}
	oldKeyID := s.writeKeyID
	keyID, key, err := generateKey()
	if err != nil {
		return err
	}
	// 先落密钥文件，再记任务行：崩溃后若任务行存在则密钥文件必然存在。
	if err := writeKeyFile(s.keyDir, keyID, key); err != nil {
		return err
	}
	if err := insertRotationTask(s.db, taskID, keyID, oldKeyID); err != nil {
		removeKeyFiles(s.keyDir, []string{keyID})
		return err
	}
	s.keys[keyID] = key
	s.rot = &rotationState{taskID: taskID, newKeyID: keyID, oldKeyID: oldKeyID}
	s.writeKeyID = keyID
	s.wg.Add(1)
	go s.runMigration(s.rot)
	return nil
}

// rotationStatus 实现 RotationStatus。
func (s *Store) rotationStatus(taskID string) (RotationResult, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return RotationResult{}, ErrClosed
	}
	s.mu.RUnlock()
	row, err := getRotationTask(s.db, taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return RotationResult{}, ErrTaskNotFound
	}
	if err != nil {
		return RotationResult{}, err
	}
	res := RotationResult{
		TaskID:      row.TaskID,
		State:       row.State,
		NewKeyID:    row.NewKeyID,
		CompletedAt: row.CompletedAt,
	}
	// Migrated/Remaining 反映当前记录在该任务新旧密钥上的实时分布。
	migrated, err := countRecordsOnKey(s.db, row.NewKeyID)
	if err != nil {
		return RotationResult{}, err
	}
	remaining, err := countRecordsNotOnKey(s.db, row.NewKeyID)
	if err != nil {
		return RotationResult{}, err
	}
	res.Migrated = migrated
	res.Remaining = remaining
	return res, nil
}

// runMigration 后台逐批把旧密钥记录重加密到新密钥，全部完成后收尾。
func (s *Store) runMigration(rot *rotationState) {
	defer s.wg.Done()
	for {
		select {
		case <-s.doneCh:
			return
		default:
		}
		done, err := s.migrateBatch(rot)
		if err != nil {
			// 关闭过程中数据库已释放，直接退出；其余错误短暂等待后重试。
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed {
				return
			}
			if !sleepOrDone(s.doneCh, 100*time.Millisecond) {
				return
			}
			continue
		}
		if done {
			if err := s.finalizeRotation(rot); err != nil {
				s.mu.RLock()
				closed := s.closed
				s.mu.RUnlock()
				if closed {
					return
				}
				if !sleepOrDone(s.doneCh, 100*time.Millisecond) {
					return
				}
				continue
			}
			return
		}
	}
}

// migrateBatch 迁移一批记录；返回是否已无剩余记录。
func (s *Store) migrateBatch(rot *rotationState) (bool, error) {
	rows, err := selectRecordsNotOnKey(s.db, rot.newKeyID, migrateBatchSize)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return true, nil
	}
	for _, rec := range rows {
		s.mu.RLock()
		oldKey, ok := s.keys[rec.KeyID]
		newKey := s.keys[rot.newKeyID]
		s.mu.RUnlock()
		if !ok {
			// 记录引用了未知密钥：无法继续，等待恢复人工排查。
			return false, errKeyMissing
		}
		plain, err := decrypt(oldKey, rec.Ciphertext)
		if err != nil {
			return false, err
		}
		cipher, err := encrypt(newKey, plain)
		if err != nil {
			return false, err
		}
		// CAS：若员工在此期间重新保存了该客户，放弃本次迁移，保留新值。
		if _, err := reencryptCAS(s.db, rec.CustomerID, rec.Ciphertext, rec.KeyID, cipher, rot.newKeyID); err != nil {
			return false, err
		}
	}
	return false, nil
}

// finalizeRotation 在全部记录均已使用新密钥后收尾：
// 先移除旧密钥文件，再于同一事务内切换当前密钥并标记任务完成。
func (s *Store) finalizeRotation(rot *rotationState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	// 与 Save 串行化：持写锁复查，确保没有刚写入的旧密钥记录。
	remaining, err := countRecordsNotOnKey(s.db, rot.newKeyID)
	if err != nil {
		return err
	}
	if remaining != 0 {
		return errRotationNotReady
	}
	ids, err := listKeyIDs(s.keyDir)
	if err != nil {
		return err
	}
	var stale []string
	for _, id := range ids {
		if id != rot.newKeyID {
			stale = append(stale, id)
		}
	}
	if err := removeKeyFiles(s.keyDir, stale); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO meta(key, value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		metaKeyCurrentKeyID, rot.newKeyID); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE rotation_tasks SET state=?, completed_at=? WHERE task_id=?`,
		string(RotationCompleted), time.Now().Unix(), rot.taskID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// 内存中的旧密钥保留：并发的 Get 可能刚读到迁移前的记录，
	// 仍需用旧密钥解密。密钥文件已删除，重启后不会再加载。
	s.rot = nil
	return nil
}

// errRotationNotReady 表示收尾复查时仍有未迁移记录。
var errRotationNotReady = errors.New("taxvault: records remaining on old key")

// recoverLocked 在 Open 时根据数据库与密钥目录重建运行时状态，
// 自动继续未完成的轮换，并清理不再被引用的密钥文件。
func (s *Store) recoverLocked() error {
	current, err := getMeta(s.db, metaKeyCurrentKeyID)
	if err != nil {
		return err
	}
	active, err := getActiveRotation(s.db)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	hasActive := err == nil
	total, err := countRecords(s.db)
	if err != nil {
		return err
	}
	if current == "" && !hasActive {
		if total > 0 {
			return errors.New("taxvault: database has records but no current key metadata")
		}
		// 全新库：生成初始密钥。
		keyID, key, err := generateKey()
		if err != nil {
			return err
		}
		if err := writeKeyFile(s.keyDir, keyID, key); err != nil {
			return err
		}
		if err := setMeta(s.db, metaKeyCurrentKeyID, keyID); err != nil {
			removeKeyFiles(s.keyDir, []string{keyID})
			return err
		}
		s.keys[keyID] = key
		s.writeKeyID = keyID
		return s.removeUnreferencedKeysLocked(map[string]bool{keyID: true})
	}
	// 收集仍被引用的密钥：写入密钥（进行中轮换的新密钥，否则当前密钥）
	// 以及记录实际使用的密钥。meta 中残留的其他旧密钥不再加载。
	needed := map[string]bool{}
	if hasActive {
		needed[active.NewKeyID] = true
	} else if current != "" {
		needed[current] = true
	}
	recordKeys, err := distinctRecordKeyIDs(s.db)
	if err != nil {
		return err
	}
	for _, id := range recordKeys {
		needed[id] = true
	}
	for id := range needed {
		key, err := readKeyFile(s.keyDir, id)
		if err != nil {
			return err
		}
		s.keys[id] = key
	}
	if hasActive {
		s.rot = &rotationState{taskID: active.TaskID, newKeyID: active.NewKeyID, oldKeyID: active.OldKeyID}
		s.writeKeyID = active.NewKeyID
	} else {
		s.writeKeyID = current
	}
	if err := s.removeUnreferencedKeysLocked(needed); err != nil {
		return err
	}
	if s.rot != nil {
		rot := s.rot
		s.wg.Add(1)
		go s.runMigration(rot)
	}
	return nil
}

// removeUnreferencedKeysLocked 删除密钥目录中不再被引用的密钥文件。
func (s *Store) removeUnreferencedKeysLocked(needed map[string]bool) error {
	ids, err := listKeyIDs(s.keyDir)
	if err != nil {
		return err
	}
	var stale []string
	for _, id := range ids {
		if !needed[id] {
			stale = append(stale, id)
		}
	}
	return removeKeyFiles(s.keyDir, stale)
}

// sleepOrDone 等待 d 或超时；超时返回 true，收到停止信号返回 false。
func sleepOrDone(done <-chan struct{}, d time.Duration) bool {
	select {
	case <-done:
		return false
	case <-time.After(d):
		return true
	}
}
