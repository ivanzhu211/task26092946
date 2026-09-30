package taxvault

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const keyFileExt = ".key"

// keyFileName 返回密钥文件名；keyID 本身即安全文件名（hex 编码的随机串）。
func keyFileName(keyID string) string {
	return keyID + keyFileExt
}

// loadKeys 从密钥目录加载全部密钥，按 keyID 返回。
func loadKeys(dir string) (map[string][]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string][]byte{}, nil
		}
		return nil, fmt.Errorf("read key dir: %w", err)
	}
	keys := make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), keyFileExt) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read key file %s: %w", e.Name(), err)
		}
		key, err := hex.DecodeString(string(bytes.TrimSpace(data)))
		if err != nil {
			return nil, fmt.Errorf("decode key file %s: %w", e.Name(), err)
		}
		if len(key) != keySize {
			return nil, fmt.Errorf("key file %s: invalid key size %d", e.Name(), len(key))
		}
		keys[strings.TrimSuffix(e.Name(), keyFileExt)] = key
	}
	return keys, nil
}

// writeKey 将密钥以 hex 编码原子写入密钥目录（临时文件 + rename + fsync）。
func writeKey(dir, keyID string, key []byte) error {
	if len(key) != keySize {
		return fmt.Errorf("invalid key size %d", len(key))
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp key file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.WriteString(hex.EncodeToString(key)); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp key file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp key file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp key file: %w", err)
	}
	final := filepath.Join(dir, keyFileName(keyID))
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("rename key file: %w", err)
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// removeKey 从密钥目录删除密钥文件；文件不存在视为成功。
func removeKey(dir, keyID string) error {
	err := os.Remove(filepath.Join(dir, keyFileName(keyID)))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove key file: %w", err)
	}
	return nil
}

// sortedKeyIDs 返回排序后的 keyID 列表，便于确定性处理。
func sortedKeyIDs(keys map[string][]byte) []string {
	ids := make([]string, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
