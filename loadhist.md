# loadhist v2 设计方案

> 从下载 → 清洗 → 写入 duckport-rs 热库的全新设计。
> 核心目标：幂等、增量、原子、可重跑。

---

## 一、核心假设

**duck_time 之前的数据已完整且准确，无需补录。**

为保留 1 天冗余缓冲（应对边界行缺失、时钟漂移等），实际导入边界取 `duck_time - 1 day`，重叠部分由 `ON CONFLICT DO NOTHING` 幂等处理。

---

## 二、现有方案的问题

| 问题 | 现象 |
|------|------|
| 全量重处理 | 每次重跑对所有 Parquet 文件走完整 RPC 链路，历史越多越慢 |
| 清洗无增量 | 每次对所有 Parquet 重跑清洗，旧文件重复处理 |
| Parquet 原地写 | `df.to_parquet(file)` 直接覆盖，进程崩溃时文件损坏且不可恢复 |
| 资源泄漏 | `duckdb.connect()` 在异常路径未 close |
| 时区不一致 | DuckDB `read_parquet` 返回 tz-aware，pandas 返回 tz-naive，比较报错 |
| 双扫描 | `get_all_trading_range` + `find_useless_symbols` 各扫一遍全目录 |
| 步骤无隔离 | 下载失败后清洗/导入仍继续对残缺数据集执行 |

---

## 三、整体流程

```
Step 0  ping + init_schema
   │
Step 1  下载（基本不变，已有 skip-existing 逻辑）
   │      monthly zip → batch_process_data → monthly Parquet
   │      daily   zip → batch_process_data → 合并到 monthly Parquet
   │
Step 2  增量清洗（每个文件只清洗一次）
   │      一次全目录扫描 → 计算 trading_range + useless_symbols（合并查询）
   │      对每个 Parquet：
   │        .ok marker 存在且 mtime 未变 → skip
   │        否则：clean → 写 .tmp → os.replace() → 更新 marker
   │
Step 3  增量导入
   │      读 duck_time（来自 duckport-rs）
   │      boundary = duck_time - 1 day（冗余缓冲）
   │      对每个 Parquet（按文件名时间排序）：
   │        file_max < boundary  →  skip
   │        file_max >= boundary →  WHERE open_time >= boundary
   │                                ON CONFLICT DO NOTHING 处理重叠
   │      更新 duck_time = max(本次加载的 open_time)
   │
完成提示：告知用户 Parquet 目录路径，可手动删除释放磁盘空间
```

---

## 四、Step 2：增量清洗

### 合并扫描 + `.ok` marker + 原子写

```
{market}_2024-12.parquet       ← 数据文件
{market}_2024-12.parquet.ok    ← marker：记录清洗时该文件的 mtime（浮点秒）
```

**跳过条件**：marker 存在且记录的 mtime 与当前文件 mtime 一致 → 文件未变，跳过。

**原子写**：写 `{file}.tmp` → `os.replace(tmp, file)` → 更新 marker。进程崩溃只留下 `.tmp`，原文件不损坏。

---

## 五、Step 3：增量导入

### 判断逻辑

```
boundary = duck_time - 1 day（duck_time 为 None 时 boundary 也为 None）

file_max  < boundary   →  skip（文件内所有数据均早于边界，已在热库）
file_max >= boundary   →  WHERE open_time >= boundary
                           ON CONFLICT DO NOTHING（处理冗余 1 天重叠）
boundary 为 None       →  无 WHERE，全量写入（首次运行）
```

### 场景验证

| 场景 | file_max | duck_time | boundary | 行为 |
|------|----------|-----------|----------|------|
| 首次运行 | 任意 | None | None | 全量写入 ✓ |
| 重跑，旧月份 | 2024-03-31 | 2025-01-20 | 2025-01-19 | skip ✓ |
| 重跑，当月无新数据 | 2024-12-15 | 2024-12-15 | 2024-12-14 | 加载 Dec 14~15（ON CONFLICT 去重）✓ |
| 重跑，当月有新 daily | 2024-12-20 | 2024-12-15 | 2024-12-14 | 加载 Dec 14~20，Dec 14~15 ON CONFLICT ✓ |
| 新月份文件 | 2025-02-28 | 2025-01-31 | 2025-01-30 | 加载 Jan 30 起全部数据 ✓ |

---

## 六、配置项

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `LOADHIST_BOUNDARY_DAYS` | `1` | 冗余缓冲天数（`duck_time - N day` 作为导入起点） |

Parquet 文件导入后**默认保留**（不删除），程序结束时输出提示告知用户可手动删除。

---

## 七、与现有代码对比

| 维度 | 现有 loadhist | v2 |
|------|---------------|----|
| 重跑性能 | 全量重走所有文件 | `file_max < boundary` 直接跳过 |
| 增量边界 | 无 | `duck_time - 1 day` |
| 冗余保障 | 无 | 1 天重叠，ON CONFLICT 幂等处理 |
| Parquet 写 | 直接覆盖，崩溃损坏 | `.tmp` + `os.replace()`，原子 |
| 清洗增量 | 每次重跑所有文件 | `.ok` marker，未变化文件跳过 |
| 时区处理 | tz-aware vs tz-naive 混用 | 清洗阶段统一 `tz_convert(None)` |
| 资源安全 | 异常时 `local_conn` 泄漏 | `try/finally` 保证关闭 |
| 扫描次数 | 两次独立 `read_parquet(glob)` | 一次合并 GROUP BY |
| 配置复杂度 | 5 个相关环境变量 | 1 个（`LOADHIST_BOUNDARY_DAYS`） |
