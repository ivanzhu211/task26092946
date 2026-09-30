package taxvault_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"taxvault/taxvault"
)

func tempDir() string {
	dir, err := os.MkdirTemp("", "taxvault-example")
	if err != nil {
		panic(err)
	}
	return dir
}

func Example() {
	dir := tempDir()
	s, err := taxvault.Open(dir+"/vault.db", dir+"/keys")
	if err != nil {
		panic(err)
	}
	defer s.Close()
	ctx := context.Background()

	// 保存与读取
	if err := s.Save(ctx, "client-001", "91310000MA1K3X0000"); err != nil {
		panic(err)
	}
	taxID, found, _ := s.Get(ctx, "client-001")
	fmt.Println(taxID, found)

	// 发起轮换（任务标识由调用方保证唯一）
	res, err := s.StartRotation(ctx, "rotation-2026-10")
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Status)

	// 等待并查看轮换结果
	for {
		res, _, _ := s.RotationStatus(ctx, "rotation-2026-10")
		if res.Status == taxvault.RotationCompleted {
			fmt.Println(res.Status, res.Migrated)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Output:
	// 91310000MA1K3X0000 true
	// in_progress
	// completed 1
}
