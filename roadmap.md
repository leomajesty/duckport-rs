# duckport-rs Roadmap

> DuckDB gRPC 数据库服务 — 通过 Arrow Flight 协议实现多进程读写

---

## 已完成

### Plan A — 单热库 + 关闭 Parquet 混合读 / 默认关闭周期 retention ✅

- **消费端**（`client/duckport_consumer`）：`get_market` / `get_symbol` 仅对 `data.kline_*` 做 resample，不再走 `read_parquet` hybrid；`pqt_path` / `redundancy_hours` 保留为兼容入参，首次使用时打弃用警告。
- **服务端**（`duckport-server`）：`DUCKPORT_RETENTION_ENABLED` **默认** `false`，不启动 `retention` 周期任务。
- **ingestor**：`RETENTION_ENABLED` **默认** `false`，不注册 `data.retention_tasks`；`DuckportClient.execute_retention` 默认 no-op；需旧版行为时设 `RETENTION_ENABLED=true` 且 `DUCKPORT_RETENTION_ENABLED=true`。
- **文档**：`deploy.md` / `ingestor/config.env.example` / `plan-a-implementation.md` 已同步。

### Phase 1 — Airport 读平面 ✅

将 DuckDB 作为 `airport-rs` catalog 暴露，DuckDB Airport 扩展客户端可通过 `ATTACH 'grpc://...' AS dp (TYPE AIRPORT)` 直接执行 SELECT。

- `DuckDbCatalog` / `DuckDbSchema` / `DuckDbTable` 适配 airport trait
- r2d2 读连接池 + `spawn_blocking` 异步桥接
- 动态 `catalog_epoch` + `is_fixed: false` 实现客户端 catalog 缓存刷新
- 隐藏 `main` schema 避免 Airport 扩展 SQL planner 冲突

### Phase 2a — 自定义写平面 (DoAction) ✅

通过 `DuckportService` 拦截 `duckport.*` 前缀的 Flight DoAction，其余请求透传给 airport。


| Action                         | 说明                                |
| ------------------------------ | --------------------------------- |
| `duckport.ping`                | 健康探针，返回 server/duckdb 版本          |
| `duckport.execute`             | 单语句写 (DDL/DML)，返回 `rows_affected` |
| `duckport.execute_transaction` | 多语句原子执行 (BEGIN/COMMIT/ROLLBACK)   |


- 所有写操作经 `Backend::with_writer` 序列化，符合 DuckDB 单写模型
- 写成功后 `bump_catalog_epoch()`，Airport 客户端自动刷新 schema

### Phase 2b — 批量 Arrow 追加 (DoPut) ✅

descriptor path = `["duckport.append", <schema>, <table>]`

- 接收完整 Arrow IPC 流，全量缓存后在 writer 上 `BEGIN` → `Appender` → `COMMIT`
- 任何错误整体 ROLLBACK，保证原子性
- 非 `duckport.append` 的 DoPut 返回 `Unimplemented`

### Phase 4 — Ingestor 写入路径迁移 ✅

Python 侧 `DuckportClient` 封装 + `DataJobs` 集成，将 kline 写入从本地 DuckDB 迁移到远程 duckport-rs。

- **Staging table 模式**：DoPut → staging (无 PK) → `execute_transaction` 原子移入目标表 (ON CONFLICT DO NOTHING) + watermark 更新
- `write_kline` / `save_exginfo` / `execute_retention` 完整迁移
- 通过 Airport DoGet 读回 `config_dict` 获取 `duck_time`
- 业务表放 `data` schema（Airport 可见），staging 表放 `main`（仅 SQL 访问）
- 显式 Arrow Schema 处理 Null 列（`contract_type` / `margin_asset`）和 Decimal → str 转换

### Phase 4b — binance-ingestor 独立包 ✅

将 Binance 数据采集逻辑从 `duckport` 单体中抽取为独立 Python 包 `binance-ingestor`。

- 独立入口 `python -m binance_ingestor.main`
- **完全移除本地 DuckDB 依赖**：无 `db_manager`、无 `import duckdb`
- 移除 `FlightActions` / `FlightGets`，`duck_time` 从 `DuckportClient.read_duck_times()` 初始化
- `config.py` 改为纯 env 驱动（移除 argparse），新增 `DUCKPORT_ADDR` / `DUCKPORT_SCHEMA`
- 支持 REST 轮询模式（`RestfulDataJobs`）和 WebSocket 实时模式（`WebsocketsDataJobs`）
- 包含完整 Binance API 层（REST + WS）、权重管理、交易对过滤、分片 WS 监听

### Phase 4c — loadhist 历史数据导入迁移 ✅

将 `loadhist.py` 及其依赖的 `hist/` 模块完整迁入 `binance-ingestor`，移除对本地 `KlineDBManager` / `duckdb.connect(DUCKDB_DIR)` 的依赖。

- `hist/` 模块（symbol_manager / data_lister / downloader / file_manager）迁入 `binance_ingestor/hist/`，`from utils.config` → `from binance_ingestor.config`
- `config.py` 扩展：新增 `RESOURCE_PATH`、`BASE_URL`、`root_center_url`、`retry_times` 等 hist 下载配置
- `date_partition.py` 补充 `get_parquet_cutoff_date()` 函数
- `DuckportClient.bulk_write_kline()`：分块 DoPut staging-table 模式，200K 行/块，适配大批量历史写入
- `binance_ingestor/loadhist.py` 新入口（`python -m binance_ingestor.loadhist`）：
  - Step 0: `DuckportClient.init_schema()` 远程建表
  - Step 1: `hist` 模块下载 + zip → Parquet 转换（逻辑不变）
  - Step 2: `duckdb.connect()` 内存模式分析本地 Parquet 并清理（DuckDB 仅作查询引擎，不创建文件）
  - Step 3: 本地读 Parquet → Arrow → `bulk_write_kline()` DoPut 到 duckport-rs
- pyproject.toml 新增 `loadhist` CLI 入口 + `duckdb`/`lxml`/`aiofiles`/`tqdm`/`joblib` 依赖

**端到端测试 9/9 通过**：分块写入、幂等性、Parquet 全链路、DuckDB 内存 Parquet 分析

### Phase 6a — 消费方读路径迁移 ✅

在 Rust 侧新增自定义 `duckport.query` DoGet，在 Python 侧新建独立 `consumer-client` 包，替代旧 Python FlightServer 的消费方读路径。

**Rust 侧：**

- `proto.rs` 新增 `QueryTicket` 结构 (`{"type": "duckport.query", "sql": "..."}`)
- 新建 `write_plane/query.rs`：reader 连接池执行 SQL → `mpsc::channel` → `FlightDataEncoderBuilder` 流式返回
- `mod.rs` `do_get` 拦截 `duckport.query` ticket，其余透传 Airport
- SQL 安全校验：仅允许 `SELECT` / `WITH` / `EXPLAIN`，阻止 DDL/DML

**Python 侧 `consumer-client/`：**

- `resample.py`：主路径为 `build_duckdb_resample_sql`；Parquet / hybrid 构造器仍保留在模块内供自定义脚本
- `DuckportConsumer` 类：`query()` / `ping()` / `read_duck_time()` / `get_market()` / `get_symbol()` / `get_exginfo()`
- 方法签名与旧 `FlightClient` 完全对齐，消费方一行改连接地址即可迁移
- **Plan A**：`get_market` / `get_symbol` 仅查热表；`kline_prefix` 仍可用；`pqt_path` / `redundancy_hours` 保留入参但已弃用（打警告）

**端到端测试 19/19 通过**：原始 DoGet、DuckportConsumer 高层 API、SQL 构造器单元测试

### Phase 7 — Python FlightServer 下线前置项 ✅

完成全部前置项，Python FlightServer 可安全下线。

- `ingestor/start_ingestor.py`：替代 `start_server.py` 的新入口，调用 `binance_ingestor.main:main`
- `flight_data_jobs.py`：移除 `db_manager` / `KlineDBManager` 全部引用和 if/else 双路径，`DuckportClient` 成为唯一写路径
- `flight_server.py`：构造 `DuckportClient` 并传入 `DataJobs`，不再向 `DataJobs` 传递 `db_manager`
- `test_phase4_ingestor.py`：改为 `from binance_ingestor.duckport_client import DuckportClient`

---

## 业务解耦完成度


| 组件                        | 状态   | 说明                                                                 |
| ------------------------- | ---- | ------------------------------------------------------------------ |
| duckport-rs 读平面           | ✅ 完成 | Airport 协议，客户端可直接 SELECT                                           |
| duckport-rs 写平面           | ✅ 完成 | DoAction + DoPut，单 writer 序列化                                      |
| binance-ingestor 独立包      | ✅ 完成 | 独立运行，仅依赖 DuckportClient RPC                                        |
| kline 写入迁移                | ✅ 完成 | write_kline via staging table 模式                                   |
| exginfo 写入迁移              | ✅ 完成 | save_exginfo via staging table 模式                                  |
| retention 迁移              | ✅ 完成 | 代码路径保留；**Plan A 下默认不执行** 周期 COPY+DELETE（见 `RETENTION_ENABLED` / `DUCKPORT_RETENTION_ENABLED`） |
| duck_time 读取迁移            | ✅ 完成 | 从 DuckportClient.read_duck_times() 初始化                             |
| loadhist.py 迁移            | ✅ 完成 | `bulk_write_kline` + `binance_ingestor/loadhist.py`                |
| Python FlightServer 下线前置项 | ✅ 完成 | `start_ingestor.py` 新入口 + `db_manager` 双路径已移除 + 测试已切换              |
| 消费方读路径迁移                  | ✅ 完成 | `duckport.query` DoGet + `consumer-client` Python 包                |
| 旧代码双路径清理                  | ✅ 完成 | `flight_data_jobs.py` 中 `db_manager` if/else 已移除，仅走 DuckportClient |


---

## 进行中 / 计划

### Phase 2c — 参数化 SQL

`duckport.execute` 支持 `params: Vec<JsonValue>`，避免客户端手动拼接字面量。

- `ExecuteRequest` 新增 `params` 字段
- JSON → DuckDB Value 类型映射 (string/int/float/bool/null/timestamp)
- `execute_transaction` 同步支持参数化

### Phase 3 — 可观测性 & 运维

- HTTP `/healthz` + `/readyz` 端点 (tonic-health 或独立 axum)
- Prometheus 指标：`duckport_append_rows_total`, `duckport_write_duration_seconds`, `duckport_write_rollback_total`, `duckport_read_pool_active`
- 优雅关停 (SIGTERM → drain inflight → close DuckDB)
- Token 认证：`Authorization: Bearer <secret>` tonic interceptor

### Phase 5 — 生产加固

- TLS 支持 (tonic `tls-ring` feature 已在 airport Cargo.toml 中声明)
- 连接级速率限制 / 并发写队列上限
- DuckDB WAL checkpoint 策略配置
- `COPY TO` parquet 路径安全校验（防止路径遍历）
- CI 流水线：cargo test + Python e2e 测试自动化

### Phase 6 — 查询增强

- ~~自定义 DoGet ticket 支持原始 SQL（绕过 Airport catalog 层）~~ → Phase 6a `duckport.query` DoGet 已实现
- `duckport.query` DoAction：执行 SELECT 并返回 JSON 结果（轻量查询场景，无需 Arrow 序列化）
- Airport DoGet 支持 filter pushdown（解析 DuckDB 序列化表达式 → SQL WHERE 子句）

### 远期方向

- **多数据库实例**：单进程服务多个 DuckDB 文件，通过 catalog 路由
- **增量同步**：基于 watermark 的 CDC-like 变更流（DoExchange 双向流）
- **Kubernetes Operator**：StatefulSet + PVC 管理 DuckDB 持久化

---

## 项目结构

```
duckport-rs/
├── Cargo.toml                          # workspace 根
├── crates/
│   ├── airport/                        # airport-rs (vendored fork, Arrow 58)
│   └── duckport-server/
│       └── src/
│           ├── main.rs                 # 入口：配置 → Backend → Airport + DuckportService
│           ├── config.rs               # DUCKPORT_* 环境变量
│           ├── backend/mod.rs          # DuckDB 连接管理 (读池 + 单写)
│           ├── airport_adapter/        # airport trait 适配
│           │   ├── catalog.rs
│           │   ├── schema.rs
│           │   └── table.rs
│           └── write_plane/            # 自定义 RPC（写 + 查询）
│               ├── mod.rs              # FlightService 拦截/转发
│               ├── actions.rs          # DoAction: ping/execute/execute_transaction
│               ├── put.rs              # DoPut: duckport.append
│               ├── query.rs            # DoGet: duckport.query (任意只读 SQL)
│               └── proto.rs            # JSON 请求/响应/ticket 类型
├── ingestor/                           # 独立 Python 数据采集包 (binance-ingestor)
│   ├── pyproject.toml
│   ├── start_ingestor.py               # 便捷启动脚本 (替代旧 start_server.py)
│   ├── config.env.example
│   └── binance_ingestor/
│       ├── main.py                     # 实时采集入口
│       ├── loadhist.py                 # 历史数据批量导入入口
│       ├── config.py                   # 纯 env 配置 (含 hist 下载参数)
│       ├── duckport_client.py          # Flight RPC 客户端 (含 bulk_write_kline)
│       ├── data_jobs.py                # 采集调度 (仅 DuckportClient)
│       ├── bus.py / filter_symbol.py   # 交易对映射与过滤
│       ├── hist/                       # 历史数据下载 + Parquet 转换
│       │   ├── symbol_manager.py
│       │   ├── data_lister.py
│       │   ├── downloader.py
│       │   └── file_manager.py
│       ├── api/                        # Binance REST + WebSocket API
│       ├── component/                  # candle_fetcher + candle_listener
│       └── utils/                      # log_kit, timer, date_partition, ...
├── client/                             # 独立 Python 消费方客户端包 (consumer-client)
│   ├── pyproject.toml
│   └── duckport_consumer/
│       ├── __init__.py
│       ├── client.py                   # DuckportConsumer 类
│       └── resample.py                 # resample SQL 构造器
├── tests/
│   └── python/                         # 端到端 Python 测试
│       ├── test_phase1_airport_read.py
│       ├── test_phase2a_write_plane.py
│       ├── test_phase2b_append.py
│       ├── test_phase4_ingestor.py
│       ├── test_consumer_read.py       # 消费方读路径 e2e (19 tests)
│       └── test_loadhist.py            # loadhist 迁移 e2e (9 tests)
├── roadmap.md
└── extra-enhancement.md                # 并发模型设计备忘
```

---

## 依赖版本锁定


| 组件                   | 版本                          |
| -------------------- | --------------------------- |
| DuckDB (Rust crate)  | `=1.10501.0` (DuckDB 1.5.1) |
| Arrow / Arrow Flight | 58                          |
| tonic (gRPC)         | 0.14                        |
| prost (protobuf)     | 0.14                        |
| Rust edition         | 2021, MSRV 1.85.1           |


