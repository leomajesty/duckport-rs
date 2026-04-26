# duckport-rs

把 DuckDB 包装成一个 **gRPC 数据库服务**，通过 Arrow Flight 协议提供多进程读写能力。

- **读平面**：通过 [`airport-rs`](./crates/airport/README.md) 实现 Airport 协议，DuckDB 客户端可直接 `ATTACH 'grpc://...' AS dp (TYPE AIRPORT)` 并执行任意 SELECT。
- **写平面**：自定义 `duckport.*` 前缀的 Flight DoAction + DoPut，在 DuckDB 单写入模型上提供显式事务、批量 Arrow 追加等能力。

核心动机是让 DuckDB 摆脱"同进程嵌入"的限制，作为轻量级数据服务供上下游多个业务进程共享。

---

## 架构总览

```
┌─────────────────┐        ┌─────────────────┐        ┌────────────────┐
│ binance-ingestor│        │  consumer 客户端 │        │ DuckDB CLI /   │
│ (Python)        │        │  (Python)       │        │ DBeaver 等     │
│  写 + 读         │        │  读             │        │  读 (Airport)   │
└────────┬────────┘        └────────┬────────┘        └────────┬───────┘
         │DoPut/DoAction            │DoGet                     │Airport
         │ (duckport.*)             │(duckport.query)          │ (原生)
         └──────────────┬───────────┴──────────────────────────┘
                        │ Arrow Flight (gRPC)
                        ▼
              ┌──────────────────────────────────────┐
              │        duckport-server (Rust)         │
              │                                       │
              │  ┌────────────────┐  ┌─────────────┐ │
              │  │ DuckportService│  │  Airport    │ │
              │  │  (write plane) │──▶  (read      │ │
              │  │ 拦截 duckport.* │  │  plane)     │ │
              │  └───────┬────────┘  └──────┬──────┘ │
              │          │                  │        │
              │  ┌───────▼──────────────────▼──────┐ │
              │  │   Backend                       │ │
              │  │   ├─ writer (Mutex<Connection>) │ │
              │  │   └─ reader pool (r2d2)         │ │
              │  └───────┬─────────────────────────┘ │
              │          │ duckdb FFI                │
              │          ▼                           │
              │      duckport.db (DuckDB 1.5.1)      │
              └──────────────────────────────────────┘
```

关键设计：

- **单一 DuckDB 实例**：`Backend::open` 打开一次 DB 文件，writer 和 reader 都通过 `try_clone()` 共享同一 `Database` 实例。保证 writer 的 commit 对 reader 立即可见。
- **单写串行化**：writer 连接包在 `tokio::sync::Mutex` 里，所有 write RPC FIFO 执行，符合 DuckDB 单写模型。
- **读并发**：r2d2 连接池，池内连接都是 writer 的 `try_clone`，共享 MVCC 状态。
- **Catalog epoch**：任何写 RPC 后 `bump_catalog_epoch()`，Airport 客户端据此刷新 schema 缓存。

详见 `extra-enhancement.md` 关于并发模型的决策记录。

---

## 项目结构

```
duckport-rs/
├── Cargo.toml                    # cargo workspace 根
├── crates/
│   ├── airport/                  # airport-rs (vendored fork, Arrow 58)
│   │   ├── src/                  # Airport 协议的通用 server/catalog trait 实现
│   │   └── README.md             # 原库说明
│   └── duckport-server/          # duckport 服务端二进制
│       └── src/
│           ├── main.rs           # 入口：加载 config → 构建 Backend → 启动 tonic
│           ├── config.rs         # 所有 DUCKPORT_* 环境变量解析
│           ├── backend/mod.rs    # DuckDB 连接池 + writer Mutex + catalog epoch
│           ├── airport_adapter/  # airport trait 的 DuckDB 实现
│           │   ├── catalog.rs    # duckdb_schemas() → airport::Catalog
│           │   ├── schema.rs     # duckdb_tables() → airport::Schema
│           │   └── table.rs      # 表扫描 + Arrow IPC 编码
│           └── write_plane/      # 自定义 duckport.* RPC
│               ├── mod.rs        # FlightService 拦截/转发逻辑
│               ├── actions.rs    # DoAction: ping / execute / execute_transaction
│               ├── put.rs        # DoPut:   duckport.append (bulk Arrow)
│               ├── query.rs      # DoGet:   duckport.query (任意只读 SQL)
│               └── proto.rs      # JSON 请求/响应类型
│
├── ingestor/                     # Python 数据采集包 binance-ingestor（独立 PyPI 包）
│   ├── pyproject.toml
│   ├── start_ingestor.py         # 便捷启动脚本
│   ├── config.env.example
│   └── binance_ingestor/
│       ├── main.py               # binance-ingestor 入口
│       ├── loadhist.py           # 历史数据回填入口 (loadhist CLI)
│       ├── duckport_client.py    # Flight client 封装（KLINE 特化 schema/append 逻辑）
│       ├── data_jobs.py          # REST/WS 两种采集模式
│       ├── config.py             # 纯 env 驱动配置
│       ├── api/                  # Binance REST + WS API
│       ├── component/            # K 线 fetcher / WS listener
│       └── hist/                 # 历史数据下载 + 清洗
│
├── client/                       # Python 消费者包 duckport-consumer
│   ├── pyproject.toml
│   └── duckport_consumer/
│       ├── client.py             # DuckportConsumer (query/get_market/get_symbol)
│       └── resample.py           # K 线 resample SQL 构造器
│
├── tests/python/                 # 端到端集成测试（跨 Rust 服务 + Python 客户端）
│   ├── test_phase1_airport_read.py
│   ├── test_phase2a_write_plane.py
│   ├── test_phase2b_append.py
│   ├── test_phase4_ingestor.py
│   ├── test_loadhist.py
│   └── test_consumer_read.py
│
├── roadmap.md                    # 阶段进度 + 待办事项
├── deploy.md                     # 生产部署 + systemd + 运维手册
├── migration-guide.md            # 从旧版 Python duckport 迁移数据的指南
├── extra-enhancement.md          # 并发模型 D1a/D1b 决策
└── path-b-retention-plan.md      # 归档架构改造方案（路径 B 实施计划）
```

---

## 核心 Flight RPC 接口

所有接口通过 Arrow Flight 标准 gRPC 暴露，默认端口 `50051`。

### 1. DoAction — 控制/写操作

| Action 类型 | 请求体（JSON） | 响应体（JSON） | 说明 |
|------------|---------------|---------------|------|
| `duckport.ping` | `{}` | `{server, server_version, catalog, duckdb_version}` | 健康 + 版本探测 |
| `duckport.execute` | `{"sql": "..."}` | `{"rows_affected": N}` | 单语句 DDL/DML |
| `duckport.execute_transaction` | `{"statements": ["...", "..."]}` | `{"rows_affected": [N1, N2, ...]}` | 多语句原子执行（BEGIN/COMMIT；失败自动 ROLLBACK） |

SQL 参数化尚未支持（计划中 Phase 2c），客户端需自行拼接字面量。

### 2. DoPut — 批量 Arrow 追加

- **Descriptor path**：`["duckport.append", <schema>, <table>]`
- **Body**：标准 Arrow IPC 流（首条消息为 schema，后续为 RecordBatch）
- **语义**：整条流在单个 `BEGIN ... COMMIT` 里原子追加；任何错误整体 ROLLBACK
- **响应**：`PutResult.app_metadata` 为 JSON `{schema, table, batches, rows_appended}`

实际大规模数据需客户端分 chunk 调用（参考 `duckport_client.py:bulk_write_kline`，默认 200K 行/块）。

### 3. DoGet — 查询

两种 ticket 格式：

**a. Airport 原生表扫描**（由 airport-rs 处理）

```json
{"schema": "data", "table": "usdt_perp_5m"}
```

DuckDB airport 客户端或 `airport-rs` 客户端库自动使用。

**b. 自定义 `duckport.query`**（消费者场景）

```json
{"type": "duckport.query", "sql": "SELECT ... FROM ..."}
```

仅允许 `SELECT` / `WITH` / `EXPLAIN` 开头的语句，服务端 `query.rs` 做前缀校验。结果以 Arrow IPC 流式返回。

---

## 快速开始

### 构建

```bash
cargo build --release    # 生成 target/release/duckport-server
```

首次构建约 5 分钟（DuckDB bundled 特性）。

### 启动服务端

```bash
export DUCKPORT_DB_PATH=./duckport.db
export DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
export RUST_LOG=duckport_server=info,airport=info

./target/release/duckport-server
```

或者直接 `cargo run --bin duckport-server`。

### 健康检查

```python
import pyarrow.flight as flight, json
client = flight.FlightClient("grpc://localhost:50051")
resp = list(client.do_action(flight.Action("duckport.ping", b"")))
print(json.loads(resp[0].body.to_pybytes()))
# {'server': 'duckport', 'server_version': '0.1.0', 'catalog': 'duckport', 'duckdb_version': 'v1.5.1'}
```

### 端到端测试

```bash
cd tests/python
pip install pyarrow pandas
pytest test_phase1_airport_read.py test_phase2a_write_plane.py test_phase2b_append.py -v
```

---

## 配置参数

### 服务端（`duckport-server`）

所有参数通过环境变量传入，全部以 `DUCKPORT_` 为前缀。

| 变量 | 默认 | 说明 |
|------|------|------|
| `DUCKPORT_DB_PATH` | `./duckport.db` | DuckDB 文件路径；`:memory:` 为内存库 |
| `DUCKPORT_LISTEN_ADDR` | `0.0.0.0:50051` | Flight/gRPC 监听地址 |
| `DUCKPORT_ADVERTISED_ADDR` | 空 | 对外广播地址（用于构建 FlightEndpoint URI） |
| `DUCKPORT_CATALOG_NAME` | `duckport` | Airport 客户端 ATTACH 时看到的 catalog 名 |
| `DUCKPORT_READ_POOL_SIZE` | `4` | 读连接池大小 |
| `DUCKPORT_DUCKDB_THREADS` | `0` (DuckDB 默认) | DuckDB `threads` PRAGMA |
| `DUCKPORT_DUCKDB_MEMORY_LIMIT` | 空 (DuckDB 默认) | DuckDB `memory_limit` PRAGMA，如 `"2GB"` |
| `DUCKPORT_RETENTION_ENABLED` | `false` | Plan A：不跑周期 `COPY+DELETE`；`true` 启用旧版归档任务 |
| `DUCKPORT_RETENTION_TABLE` | `data.retention_tasks` | 任务表（ingestor 在 `RETENTION_ENABLED=true` 时写入行） |
| `DUCKPORT_SEED_DEMO` | 未设 | `=1` 时启动后植入 demo schema，便于冒烟测试 |
| `RUST_LOG` | `info` | 标准 tracing filter |

### Ingestor（Python `binance-ingestor`）

见 `ingestor/config.env.example`。关键变量：

| 变量 | 默认 | 说明 |
|------|------|------|
| `DUCKPORT_ADDR` | `localhost:50051` | 连接 duckport-server 的地址 |
| `DUCKPORT_SCHEMA` | `data` | K 线/exginfo 写入的 schema |
| `KLINE_INTERVAL` | `5m` | 基础 K 线周期 |
| `PARQUET_DIR` | `data/pqt` | loadhist / 可选旧版周期 retention 使用的归档根目录 |
| `DATA_SOURCES` | `usdt_perp,usdt_spot` | 启用的市场 |
| `CONCURRENCY` | `2` | 采集并发（REST/WS） |
| `RETENTION_ENABLED` | `false` | Plan A 默认关闭周期 COPY+DELETE；`true` 需配合 `DUCKPORT_RETENTION_ENABLED=true` |
| `RETENTION_DAYS` | `7` | 仅当 `RETENTION_ENABLED` 为真时参与调度 |

### Consumer（Python `duckport-consumer`）

构造参数（见 `client/duckport_consumer/client.py`）：

```python
from duckport_consumer import DuckportConsumer
c = DuckportConsumer(
    addr="duckport.prod:50051",
    schema="data",
    kline_interval_minutes=5,
    suffix="_5m",
    # Plan A: pqt_path / redundancy_hours 已弃用（仍仅查 data.kline_* 主表）
)
```

---

## 不同数据量级对配置的要求

DuckDB 的查询性能强依赖 **RAM**（用作 buffer pool + 查询中间状态）。数据集超过可用 RAM 时，DuckDB 会 spill to disk，延迟显著上升。下表给出经验值，实际建议基于你的查询模式（单 symbol vs 全市场，时间窗口长度）做 benchmark。

| 数据量级 | 典型场景 | 推荐 CPU/RAM | 服务端配置 |
|---------|---------|--------------|-----------|
| **< 10 GB** | 5m K 线 × 500 symbol × 3 年；POC 阶段 | 2C/4G | `READ_POOL_SIZE=4`, `DUCKDB_THREADS=2`, `DUCKDB_MEMORY_LIMIT=2GB` |
| **10–50 GB** | 5m K 线 × 500 symbol × 10 年；1m K 线 × 500 symbol × 3 年 | 4C/8G | `READ_POOL_SIZE=4`, `DUCKDB_THREADS=4`, `DUCKDB_MEMORY_LIMIT=5GB` |
| **50–200 GB** | 1m K 线 × 1000 symbol × 5 年 | 8C/16G | `READ_POOL_SIZE=6`, `DUCKDB_THREADS=6`, `DUCKDB_MEMORY_LIMIT=10GB` |
| **> 200 GB** | tick 数据 / 多品种合并 | 16C/32G+ | 考虑启用 Parquet overlay（见 `path-b-retention-plan.md`），热数据留 DuckDB，冷数据走 `read_parquet` |

### 2C/2G 低配环境（最低要求）

当前 `deploy.md` 设定的最低硬件是 2C2G，这个配置在下列约束下可用：

- DuckDB 数据 **< 10 GB**
- 查询以 **单 symbol 为主**（row group pruning 高命中率）
- **无** 全市场跨年聚合查询
- `DUCKPORT_DUCKDB_MEMORY_LIMIT=800MB`，`READ_POOL_SIZE=2`，`DUCKDB_THREADS=2`

超过这些约束会触发频繁 spill to disk，P95 延迟从秒级跌到分钟级。此时两条路：升级硬件到 4C/8G，或启用 Parquet overlay 把热数据压到几百 MB 以内。

### 写入性能调优

- DoPut 单次追加 **不要超过 ~500 MB** Arrow IPC 流（服务端全量缓存后写入），超过时分 chunk 调用
- Ingestor 侧典型 chunk 大小：**200,000 行**（经验值，见 `bulk_write_kline`）
- 大量写入后主动发送 `duckport.execute` SQL `CHECKPOINT`，将 WAL 合并到主文件

---

## 接入新的 Ingestor

基本思路：写一个 Flight client，按以下次序调用 RPC。Python / Rust / Go 的 pyarrow/arrow-flight 库都支持所需操作。

### 步骤 1：初始化 Schema

用 `duckport.execute_transaction` 一次性建表（幂等 DDL）：

```python
import json, pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:50051")

def execute_transaction(stmts):
    body = json.dumps({"statements": stmts}).encode("utf-8")
    action = flight.Action("duckport.execute_transaction", body)
    return list(client.do_action(action))

execute_transaction([
    "CREATE SCHEMA IF NOT EXISTS metrics",
    """CREATE TABLE IF NOT EXISTS metrics.events (
        ts TIMESTAMP,
        device_id VARCHAR,
        value DOUBLE,
        PRIMARY KEY (ts, device_id)
    )""",
    "CREATE TABLE IF NOT EXISTS _staging_events (ts TIMESTAMP, device_id VARCHAR, value DOUBLE)",
])
```

**约定**：

- 业务表放任意非 `main` 的 schema（`main` 被 airport_adapter 屏蔽）
- **staging 表放 `main` schema**（对 Airport 客户端不可见，但 SQL 可访问）——staging 表无 PK，供 DoPut 批量写入
- `CREATE SCHEMA IF NOT EXISTS` 必须和 `CREATE TABLE` 在**同一事务**里，否则下一语句会看不见 schema

### 步骤 2：批量写数据（staging 模式）

DuckDB Appender 不支持 `ON CONFLICT`。解决方案：**DoPut → staging → INSERT ... ON CONFLICT DO NOTHING**。

```python
import pyarrow as pa

def append_to_staging(schema, table, arrow_table):
    descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
    writer, reader = client.do_put(descriptor, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.done_writing()
    resp = json.loads(bytes(reader.read()))
    writer.close()
    return resp

arrow = pa.Table.from_pylist([
    {"ts": "2026-04-22 10:00:00", "device_id": "dev-1", "value": 1.23},
    {"ts": "2026-04-22 10:00:00", "device_id": "dev-2", "value": 4.56},
])

append_to_staging("main", "_staging_events", arrow)

execute_transaction([
    "INSERT INTO metrics.events SELECT * FROM _staging_events ON CONFLICT DO NOTHING",
    "TRUNCATE _staging_events",
])
```

**分 chunk 处理大批量**：

```python
CHUNK_SIZE = 200_000
for offset in range(0, len(arrow), CHUNK_SIZE):
    chunk = arrow.slice(offset, CHUNK_SIZE)
    execute_transaction(["TRUNCATE _staging_events"])
    append_to_staging("main", "_staging_events", chunk)
    execute_transaction([
        "INSERT INTO metrics.events SELECT * FROM _staging_events ON CONFLICT DO NOTHING",
        "TRUNCATE _staging_events",
    ])
```

### 步骤 3：更新 Watermark（如需要）

推荐在同一事务里更新 watermark，保证数据和游标原子一致：

```python
execute_transaction([
    "INSERT INTO metrics.events SELECT * FROM _staging_events ON CONFLICT DO NOTHING",
    f"INSERT OR REPLACE INTO metrics.config_dict (key, value) "
    f"VALUES ('events_latest_ts', '{latest_ts}')",
    "TRUNCATE _staging_events",
])
```

### 步骤 4：错误处理与幂等

- `duckport.execute_transaction` 任一语句失败，整个事务 ROLLBACK；客户端应捕获 `FlightError` 并重试
- DoPut 失败（schema 不匹配、类型错误）不会污染表
- `ON CONFLICT DO NOTHING` 使得同一批数据重复 DoPut 是**幂等**的，消费侧从 watermark 断点续传不会产生副作用

### 步骤 5（可选）：只读查询

消费侧的读路径通过 `duckport.query` DoGet：

```python
def query(sql):
    ticket_data = json.dumps({"type": "duckport.query", "sql": sql})
    ticket = flight.Ticket(ticket_data.encode("utf-8"))
    reader = client.do_get(ticket)
    return reader.read_all()

tbl = query("SELECT * FROM metrics.events WHERE ts >= '2026-04-22' ORDER BY ts LIMIT 100")
print(tbl.to_pandas())
```

仅支持 `SELECT` / `WITH` / `EXPLAIN`，DDL/DML 必须走 `duckport.execute`。

### 参考实现

- `ingestor/binance_ingestor/duckport_client.py`：完整的 Python 客户端封装（init_schema / write_kline / bulk_write_kline / save_exginfo / execute_retention）
- `client/duckport_consumer/client.py`：只读消费端
- `tests/python/test_phase4_ingestor.py`：端到端写入测试样板

### 新 ingestor 的项目建议

```
my-ingestor/
├── pyproject.toml              # 依赖 pyarrow>=15, pandas>=2
├── my_ingestor/
│   ├── client.py               # 封装 DuckportClient（参考 duckport_client.py）
│   ├── schema.py               # 集中管理 CREATE TABLE DDL
│   ├── fetcher.py              # 数据源抓取逻辑
│   └── main.py                 # 入口：init_schema → loop(fetch → write)
└── config.env.example
```

与 binance-ingestor 共享同一个 `duckport-server`——它们写入不同 schema / 表即可，互不冲突。

---

## 相关文档

| 文档 | 用途 |
|------|------|
| [`roadmap.md`](./roadmap.md) | 阶段进度 + 未完成待办 |
| [`deploy.md`](./deploy.md) | 生产部署、systemd、备份恢复、故障排查 |
| [`migration-guide.md`](./migration-guide.md) | 从旧版 Python duckport 迁移数据 |
| [`extra-enhancement.md`](./extra-enhancement.md) | 并发模型决策（D1a per-table actor vs D1b 单写者） |
| [`path-b-retention-plan.md`](./path-b-retention-plan.md) | Parquet 归档透明化改造方案（大数据量路径） |
| [`crates/airport/README.md`](./crates/airport/README.md) | airport-rs 库本身的使用文档 |

---

## 版本与依赖

- **Rust**：1.85.1+（edition 2021）
- **DuckDB**：1.5.1（crate `duckdb 1.10501.0`，特性 `bundled` 内联编译）
- **Arrow**：58.x（跟随 DuckDB crate 对齐）
- **tonic**：0.14 / **prost**：0.14
- **Python（ingestor + consumer）**：3.10+，`pyarrow >= 15`

DuckDB 版本升级会连锁影响 arrow 大版本（DuckDB 1.5.x → arrow 58）。workspace 里所有 arrow-* crate 都必须保持统一版本。

---

## 许可证

MIT（与 airport-rs 上游一致）。
