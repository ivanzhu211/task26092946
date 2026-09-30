package taxvault

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// metaKeyCurrentKeyID 是 meta 表中记录当前密钥 ID 的键。
const metaKeyCurrentKeyID = "current_key_id"

// openDB 打开 SQLite 并初始化表结构。
func openDB(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// WAL 允许并发读；写由 SQLite 串行化，busy_timeout 兜底。
	db.SetMaxOpenConns(16)
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tax_records (
			customer_id TEXT PRIMARY KEY,
			ciphertext  BLOB NOT NULL,
			key_id      TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS rotation_tasks (
			task_id      TEXT PRIMARY KEY,
			new_key_id   TEXT NOT NULL,
			old_key_id   TEXT NOT NULL,
			state        TEXT NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tax_records_key_id ON tax_records(key_id)`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// rotationRow 是 rotation_tasks 表的一行。
type rotationRow struct {
	TaskID      string
	NewKeyID    string
	OldKeyID    string
	State       RotationState
	CompletedAt int64
}

// upsertRecord 新增或替换一条税号密文。
func upsertRecord(db *sql.DB, customerID string, ciphertext []byte, keyID string) error {
	_, err := db.Exec(
		`INSERT INTO tax_records(customer_id, ciphertext, key_id) VALUES(?,?,?)
		 ON CONFLICT(customer_id) DO UPDATE SET ciphertext=excluded.ciphertext, key_id=excluded.key_id`,
		customerID, ciphertext, keyID)
	return err
}

// recordRow 是 tax_records 表的一行。
type recordRow struct {
	CustomerID string
	Ciphertext []byte
	KeyID      string
}

// getRecord 读取一条记录，不存在时返回 sql.ErrNoRows。
func getRecord(db *sql.DB, customerID string) (recordRow, error) {
	var r recordRow
	err := db.QueryRow(`SELECT customer_id, ciphertext, key_id FROM tax_records WHERE customer_id=?`, customerID).
		Scan(&r.CustomerID, &r.Ciphertext, &r.KeyID)
	return r, err
}

// getMeta 读取 meta 值，不存在时返回空串。
func getMeta(db *sql.DB, key string) (string, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// setMeta 写入 meta 值。
func setMeta(db *sql.DB, key, value string) error {
	_, err := db.Exec(
		`INSERT INTO meta(key, value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// insertRotationTask 记录一轮新轮换。
func insertRotationTask(db *sql.DB, taskID, newKeyID, oldKeyID string) error {
	_, err := db.Exec(
		`INSERT INTO rotation_tasks(task_id, new_key_id, old_key_id, state) VALUES(?,?,?,?)`,
		taskID, newKeyID, oldKeyID, string(RotationInProgress))
	return err
}

// getRotationTask 按任务标识查询轮换，不存在时返回 sql.ErrNoRows。
func getRotationTask(db *sql.DB, taskID string) (rotationRow, error) {
	var r rotationRow
	err := db.QueryRow(
		`SELECT task_id, new_key_id, old_key_id, state, completed_at FROM rotation_tasks WHERE task_id=?`, taskID).
		Scan(&r.TaskID, &r.NewKeyID, &r.OldKeyID, &r.State, &r.CompletedAt)
	return r, err
}

// getActiveRotation 返回唯一未完成的轮换，没有时返回 sql.ErrNoRows。
func getActiveRotation(db *sql.DB) (rotationRow, error) {
	var r rotationRow
	err := db.QueryRow(
		`SELECT task_id, new_key_id, old_key_id, state, completed_at FROM rotation_tasks WHERE state=?`,
		string(RotationInProgress)).
		Scan(&r.TaskID, &r.NewKeyID, &r.OldKeyID, &r.State, &r.CompletedAt)
	return r, err
}

// completeRotationTask 将轮换标记为完成。
func completeRotationTask(db *sql.DB, taskID string, completedAt int64) error {
	_, err := db.Exec(
		`UPDATE rotation_tasks SET state=?, completed_at=? WHERE task_id=?`,
		string(RotationCompleted), completedAt, taskID)
	return err
}

// countRecordsNotOnKey 统计尚未使用指定密钥的记录数。
func countRecordsNotOnKey(db *sql.DB, keyID string) (int64, error) {
	var n int64
	err := db.QueryRow(`SELECT COUNT(*) FROM tax_records WHERE key_id<>?`, keyID).Scan(&n)
	return n, err
}

// countRecordsOnKey 统计使用指定密钥的记录数。
func countRecordsOnKey(db *sql.DB, keyID string) (int64, error) {
	var n int64
	err := db.QueryRow(`SELECT COUNT(*) FROM tax_records WHERE key_id=?`, keyID).Scan(&n)
	return n, err
}

// countRecords 统计总记录数。
func countRecords(db *sql.DB) (int64, error) {
	var n int64
	err := db.QueryRow(`SELECT COUNT(*) FROM tax_records`).Scan(&n)
	return n, err
}

// selectRecordsNotOnKey 取一批仍使用旧密钥的记录。
func selectRecordsNotOnKey(db *sql.DB, keyID string, limit int) ([]recordRow, error) {
	rows, err := db.Query(
		`SELECT customer_id, ciphertext, key_id FROM tax_records WHERE key_id<>? LIMIT ?`, keyID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []recordRow
	for rows.Next() {
		var r recordRow
		if err := rows.Scan(&r.CustomerID, &r.Ciphertext, &r.KeyID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// reencryptCAS 仅当记录仍是迁移前读到的旧值时才替换为新密文，
// 避免覆盖员工在迁移期间刚保存的新税号。返回是否替换成功。
func reencryptCAS(db *sql.DB, customerID string, oldCiphertext []byte, oldKeyID string, newCiphertext []byte, newKeyID string) (bool, error) {
	res, err := db.Exec(
		`UPDATE tax_records SET ciphertext=?, key_id=? WHERE customer_id=? AND key_id=? AND ciphertext=?`,
		newCiphertext, newKeyID, customerID, oldKeyID, oldCiphertext)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// distinctRecordKeyIDs 返回当前记录实际使用的全部密钥 ID。
func distinctRecordKeyIDs(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT DISTINCT key_id FROM tax_records`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
