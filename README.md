# taxvault — 税号加密存储库

供上层调用的 Go 库：税号密文存 SQLite，密钥存独立本地目录。提供四类能力：

| 能力 | API |
| --- | --- |
| 保存税号（新增或替换） | `Store.Save(ctx, clientID, taxID)` |
| 读取税号 | `Store.Get(ctx, clientID) (taxID, found, err)` |
| 发起密钥轮换 | `Store.StartRotation(ctx, taskID)` |
| 查看轮换结果 | `Store.RotationStatus(ctx, taskID)` |

## 快速开始

```go
s, err := taxvault.Open("/data/vault.db", "/data/keys") // 数据库路径 + 密钥目录
if err != nil { /* ... */ }
defer s.Close()

ctx := context.Background()
_ = s.Save(ctx, "client-001", "91310000MA1K3X0000") // 新增或替换
taxID, found, err := s.Get(ctx, "client-001")       // 读取完整明文

res, err := s.StartRotation(ctx, "rotation-2026-10") // taskID 由调用方保证唯一
// 返回即已受理：此后成功的 Save 一律使用新密钥，后台协程异步迁移存量记录
res, found, err = s.RotationStatus(ctx, "rotation-2026-10") // 查询进度/结果
```

- 重新 `Open` 同一数据库与密钥目录即可继续使用；若上次退出时轮换未完成，
  会自动继续，无需人工补文件或指定密钥。
- 仅考虑单进程内并发调用与进程退出后的重启；不保证多进程同时操作。

## 设计要点

**加密**：AES-256-GCM，每条记录独立随机 nonce，密文为 `nonce||ciphertext`。
SQLite 中只有密文与密钥 ID；密钥以 hex 编码存于密钥目录的 `<keyID>.key`
（临时文件 + rename 原子写入），库内任何持久化位置都不保存明文税号或密钥。

**轮换状态机**（`rotations` 表，`active` 行至多一条）：

1. `StartRotation(taskID)`：生成新密钥并先落密钥文件，再在一个事务里登记
   `active` 轮换并切换 `meta.current_key_id`；返回前内存当前密钥已切换，
   因此受理后的 `Save` 必用新密钥。同一 `taskID` 重复提交只继续原轮换或
   返回原结果；已有进行中轮换时拒绝新任务（`ErrRotationInProgress`）。
2. 后台迁移：每批 500 条，解密旧密钥记录并用新密钥重加密，
   `UPDATE ... WHERE client_id=? AND key_id=<旧>` 条件更新——迁移期间员工
   刚保存的新值（已是新密钥）不会被覆盖回旧值；记录原地更新，客户不会
   短暂消失。迁移中旧记录照常可读、可改。
3. 收尾：确认无记录使用旧密钥后，先从密钥目录删除旧密钥文件，再把轮换
   标记为 `completed`。两步均幂等，崩溃后重开会重跑到收敛。

**崩溃恢复**（`Open` 时自动执行）：

- 存在 `active` 轮换 → 以其新密钥为当前密钥，重启后台迁移；
- 不存在 → 以 `meta.current_key_id` 为当前密钥；
- 清理所有“非当前密钥且无任何记录引用”的密钥文件（含崩溃残留的孤儿
  密钥），因此连续多次退出/重启不会积累旧密钥，也不会增加轮换轮数。
- 每次 `Save` 是单条 UPSERT 事务：已确认的保存不会丢失，未返回结果的
  保存要么完整生效要么不生效，不会留下半个值（GCM 完整性校验兜底）。

## 目录结构

```
taxvault/
  store.go     Store 入口：Open/Close/Save/Get、schema、恢复编排
  crypto.go    AES-256-GCM 加解密与密钥生成
  keystore.go  密钥目录的原子读写与删除
  rotation.go  轮换状态机、后台迁移、收尾与陈旧密钥清理
  *_test.go    功能、崩溃恢复、性能测试
```

## 运行测试

```sh
go test ./...          # 全部测试（含 10 万条规模性能测试）
go test ./... -race    # 竞态检测（性能用例在 race 下自动跳过）
go test ./... -short   # 跳过性能测试
```

崩溃恢复测试通过子进程 `os.Exit` 模拟进程在轮换任意阶段被杀，随后重新
`Open` 验证自动续跑与收敛。

## 验收规模实测（本机，Apple Silicon）

10 万条客户税号，轮换期间 50 个调用方（25 读 + 25 写）持续读写：

| 指标 | 结果 | 要求 |
| --- | --- | --- |
| 读 P95 | ≈17ms | < 200ms |
| 写 P95 | ≈17ms | < 200ms |
| 整轮轮换耗时（含与负载重叠） | ≈2.0s | — |
| 停止写入后到轮换完成 | ≈30ms | < 60s |
| 轮换后密钥文件数 | 1 | 仅当前密钥 |
| 10 万条逐条对账 | 全部等于最后确认保存的值 | — |

测试覆盖：

- 旧记录迁移与同一客户修改重叠：最终值等于最后确认保存的值；
- 新密钥已启用但迁移未完成、旧密钥清理未结束两种现场的退出与重启，
  恢复后逐条读取与独立维护的预期值一致；
- 连续两轮轮换及历史任务重试：最终只剩当前密钥，全部客户可读回；
- 连续多次崩溃/重启收敛：不增加轮换轮数、不遗漏客户、不积累旧密钥。
