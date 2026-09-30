# taxvault — 税号加密存储库

供上层调用的 Go 库：税号使用 AES-256-GCM 加密后存入 SQLite，密钥文件存放在
独立本地目录。提供四类能力：**保存税号、读取税号、发起密钥轮换、查看轮换结果**。
调用方身份已可信，库内不做登录、权限与界面。

## 快速开始

```go
s, err := taxvault.Open("/data/vault.db", "/data/keys") // 打开或创建；自动恢复未完成的轮换
if err != nil { /* ... */ }
defer s.Close()

// 保存（新增或替换），已确认成功的保存在重开后仍然有效
err = s.Save("customer-001", "91310000MA1K000000")

// 读取完整税号；客户不存在时返回 taxvault.ErrNotFound
taxID, err := s.Get("customer-001")

// 发起轮换：taskID 由调用方提供且全局唯一。
// 同一标识重复提交只继续原轮换或返回原结果，不会生成新一轮；
// 已有未完成轮换时返回 taxvault.ErrRotationBusy。
err = s.StartRotation("rotate-2026-10")

// 查看轮换结果；未知任务返回 taxvault.ErrTaskNotFound
res, err := s.RotationStatus("rotate-2026-10")
// res.State: RotationInProgress / RotationCompleted
// res.Migrated / res.Remaining: 记录在新旧密钥上的实时分布
```

## 运行测试

```sh
go test ./...          # 全部测试（含 10 万条规模性能测试）
go test -short ./...   # 跳过性能测试
go test -race ./...    # 竞态检测
```

## 设计要点

- **存储布局**：SQLite 表 `tax_records(customer_id, ciphertext, key_id)` 只存密文；
  `meta` 记录当前密钥 ID；`rotation_tasks` 记录每轮轮换的任务标识、新密钥 ID 与状态，
  已完成任务永久保留以支持幂等重试。密钥以 `key-<id>.key` 文件存于密钥目录，
  写入采用临时文件 + rename 的原子方式。
- **加密**：AES-256-GCM，随机 nonce，密文自包含（nonce||ciphertext）。每轮轮换
  生成独立新密钥；明文税号与密钥均不进入 SQLite。
- **轮换流程**：受理后立即切换写入密钥（后续保存即用新密钥），后台 goroutine
  以小批量（200 条/批）把旧密钥记录重加密。每条采用 CAS 更新
  （`WHERE customer_id=? AND key_id=? AND ciphertext=?`），员工在迁移期间保存的
  新值不会被覆盖回旧值，客户记录也不会短暂消失。全部记录迁移完成后，先删除
  旧密钥文件，再于同一事务内切换当前密钥并标记任务完成。
- **崩溃恢复**：`Open` 时根据 `rotation_tasks` 与记录实际使用的密钥 ID 重建状态，
  自动继续未完成的迁移，并清理密钥目录中不再被引用的密钥文件。已确认的写入
  依赖 WAL 提交持久化；单条保存是单事务，不会留下半个值。连续多次退出重启
  仍收敛：任务行幂等，不会增加轮换轮数或积累旧密钥。
- **并发**：仅面向单进程内并发与进程重启。`Save` 持读锁完成取钥-加密-落库，
  轮换收尾持写锁复查，二者串行保证“报告完成 ⇔ 全部记录已在新密钥下且旧密钥
  文件已移除”。SQLite 使用 WAL + busy_timeout，迁移批量小、事务短，不阻塞业务读写。

## 性能实测（Apple Silicon，10 万条记录，50 并发调用方持续读写）

| 指标 | 实测 | 要求 |
| --- | --- | --- |
| 读 P95 | ≈4.3 ms | ≤ 200 ms |
| 写 P95 | ≈7.2 ms | ≤ 200 ms |
| 轮换总耗时（含 5s 写入负载） | ≈9.0 s | — |
| 停止写入后轮换完成耗时 | ≈3.5 s | ≤ 60 s |

数据来自 `TestPerfRotationUnderLoad`，每次 `go test` 会重新输出。

## 文件结构

- `store.go` — 公开 API（Open/Close/Save/Get/StartRotation/RotationStatus）
- `rotation.go` — 轮换发起、后台迁移、收尾与崩溃恢复
- `db.go` — SQLite 表结构与全部 SQL
- `crypto.go` — AES-256-GCM 加解密与密钥生成
- `keystore.go` — 密钥目录的原子读写、列举与清理
- `store_test.go` / `rotation_test.go` / `perf_test.go` — 单元、恢复与规模测试
