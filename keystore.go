package taxvault

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// errKeyMissing 表示密钥目录中缺少被引用的密钥文件。
var errKeyMissing = errors.New("taxvault: referenced key file missing")

// keyFileExt 是密钥文件扩展名。
const keyFileExt = ".key"

// keyFileName 返回密钥 ID 对应的文件名。
func keyFileName(keyID string) string { return "key-" + keyID + keyFileExt }

// writeKeyFile 将密钥原子写入密钥目录（先写临时文件再 rename）。
func writeKeyFile(dir, keyID string, key []byte) error {
	if len(key) != keyLen {
		return fmt.Errorf("taxvault: key length %d, want %d", len(key), keyLen)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	final := filepath.Join(dir, keyFileName(keyID))
	tmp, err := os.CreateTemp(dir, ".tmp-key-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.WriteString(hex.EncodeToString(key)); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	return syncDir(dir)
}

// readKeyFile 从密钥目录读取密钥。
func readKeyFile(dir, keyID string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, keyFileName(keyID)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", errKeyMissing, keyID)
		}
		return nil, err
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(key) != keyLen {
		return nil, fmt.Errorf("taxvault: corrupt key file for %s", keyID)
	}
	return key, nil
}

// listKeyIDs 列出密钥目录中全部密钥 ID（忽略临时文件与非密钥文件）。
func listKeyIDs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "key-") || !strings.HasSuffix(name, keyFileExt) {
			continue
		}
		ids = append(ids, strings.TrimSuffix(strings.TrimPrefix(name, "key-"), keyFileExt))
	}
	sort.Strings(ids)
	return ids, nil
}

// removeKeyFiles 删除不再需要的密钥文件。
func removeKeyFiles(dir string, keyIDs []string) error {
	for _, id := range keyIDs {
		err := os.Remove(filepath.Join(dir, keyFileName(id)))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDir(dir)
}

// syncDir 落盘目录项，保证文件名变更在进程退出后仍然有效。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
