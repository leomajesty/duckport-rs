# duckport Ingestor 开发规范

> 本文档以 `duckport-binance-ingestor` 为参考实现，描述编写一个兼容 duckport 插件系统的 ingestor 所需遵循的规范。

---

## 目录

1. [仓库结构](#1-仓库结构)
2. [打包配置（pyproject.toml）](#2-打包配置-pyprojecttoml)
3. [配置模块（config.py）](#3-配置模块-configpy)
4. [duckport_client.py — RPC 封装](#4-duckport_clientpy--rpc-封装)
5. [Schema 设计规范](#5-schema-设计规范)
6. [数据写入流程（staging 模式）](#6-数据写入流程staging-模式)
7. [主入口（main.py）](#7-主入口-mainpy)
8. [历史数据入口（loadhist.py）](#8-历史数据入口-loadhistpy)
9. [水位线（Watermark）管理](#9-水位线watermark-管理)
10. [错误处理与幂等](#10-错误处理与幂等)
11. [注册到 registry.json](#11-注册到-registryjson)
12. [本地开发与调试](#12-本地开发与调试)

---

## 1. 仓库结构

仓库名约定以 `duckport-` 为前缀，包名使用下划线命名：

```
duckport-my-ingestor/
├── pyproject.toml              # 必须
├── config.env.example          # 必须（示例配置，不提交真实值）
├── .gitignore                  # 忽略 .venv/ config.env __pycache__ 等
├── my_ingestor/
│   ├── __init__.py
│   ├── config.py               # 纯 env 驱动，无副作用
│   ├── duckport_client.py      # Arrow Flight RPC 封装
│   ├── data_jobs.py            # 数据源拉取 + 写入循环
│   ├── loadhist.py             # 历史数据批量回填入口
│   └── main.py                 # 程序入口
└── start_ingestor.py           # 可选：本地直接运行的便捷脚本
```

**必须文件**：`pyproject.toml`、`config.py`、`main.py`、`loadhist.py`、`config.env.example`。

---

## 2. 打包配置（pyproject.toml）

```toml
[project]
name = "my-ingestor"
version = "0.1.0"
description = "My exchange data ingestor for duckport-rs"
requires-python = ">=3.12"
dependencies = [
    "pandas>=2.0",
    "pyarrow>=15.0",
    "aiohttp>=3.9",
    "python-dotenv>=1.0",
    "duckdb==1.5.1",            # 必须与 duckport-server 锁定同一版本
    # ... 其他依赖
]

[tool.setuptools.packages.find]
include = ["my_ingestor*"]      # 显式列出，避免 data/ 等目录误入

[project.scripts]
my-ingestor = "my_ingestor.main:main"      # 主进程入口
loadhist    = "my_ingestor.loadhist:main"  # 历史数据入口
```

**关键约定**：

- `[project.scripts]` 中必须同时声明主进程和 `loadhist` 两个脚本。
- `duckdb` 版本必须与 duckport-server 内嵌的 DuckDB 版本一致（当前 `==1.5.1`），否则 Parquet 文件格式可能不兼容。
- `requires-python = ">=3.12"`。
- `[tool.setuptools.packages.find]` 的 `include` 字段必须显式填写，防止 `data/`、`hist/` 等目录被错误打包。

**registry.json 中 `exec` 字段**须与 `[project.scripts]` 中主进程脚本名一致（如 `my-ingestor`）。

---

## 3. 配置模块（config.py）

配置完全由环境变量驱动，模块级加载，无 argparse，确保 import 无副作用：

```python
import os
from dotenv import load_dotenv

# 1. 加载实例 config.env（路径由 ingestor-run 通过 INGESTOR_ENV_FILE 注入）
_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)

# 2. 继承 duckport-server 的监听地址（server.env 不存在时静默跳过）
load_dotenv("/opt/duckport/server.env", override=False)

# 3. 推导 DUCKPORT_ADDR（勿要求用户手填）
def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"

DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"          # 业务表所在 schema，与 duckport-rs 约定固定为 data
```

**规则**：

- `load_dotenv(_env_file, override=False)`：实例 config.env 优先，不覆盖 shell 中已存在的同名变量。
- `load_dotenv("/opt/duckport/server.env", override=False)`：从服务端配置继承 `DUCKPORT_LISTEN_ADDR`，不得覆盖已有值。
- `DUCKPORT_ADDR` 由代码推导，**不暴露为用户配置项**。
- `DUCKPORT_SCHEMA` 固定为 `"data"`，不作为配置项。

### 用户配置项（config.env）

实例 config.env 只暴露用户真正需要调整的变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KLINE_INTERVAL` | `5m` | 基础 K 线周期 |
| `DATA_SOURCES` | `usdt_perp,usdt_spot` | 启用的市场，逗号分隔 |
| `CONCURRENCY` | `2` | 并发数 |
| `START_DATE` | `2009-01-03` | 历史起始日期 |
| `PROXY_URL` | *(空)* | HTTP 代理，留空直连 |
| `PARQUET_DIR` | `/data/duckport/pqt` | loadhist Parquet 目录（可覆盖） |
| `RESOURCE_PATH` | `/data/duckport/hist` | 历史文件下载临时目录（可覆盖） |

`config.env.example` 只列出上述变量，配合简短注释。不要在示例中包含任何凭据或私有地址。

---

## 4. duckport_client.py — RPC 封装

每个 ingestor 都应维护自己的 `DuckportClient`，封装以下 Arrow Flight RPC：

### 必须实现的方法

```python
class DuckportClient:
    def __init__(self, addr: str, schema: str):
        self.client = flight.FlightClient(f"grpc://{addr}")
        self.schema = schema

    def ping(self) -> dict:
        """duckport.ping — 连通性检查，启动时调用，失败则 sys.exit(1)"""

    def execute(self, sql: str) -> dict:
        """duckport.execute — 单条 DDL/DML"""

    def execute_transaction(self, statements: list[str]) -> dict:
        """duckport.execute_transaction — 多语句原子事务"""

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        """duckport.append — DoPut 批量 Arrow 写入"""

    def read_table(self, schema: str, table: str) -> pa.Table:
        """Airport DoGet — 读取业务表"""

    def init_schema(self, ...):
        """幂等建表，每次启动调用"""

    def read_duck_time(self, market: str) -> Optional[pd.Timestamp]:
        """读取 config_dict 中的 {market}_duck_time 水位"""

    def bulk_write_kline(self, arrow_table, market, interval, duck_time_str, ...):
        """历史批量写入，分块 200K 行，sync_duck_time 可延迟"""

    def replace_duck_time(self, market: str, duck_time_str: str):
        """单独更新水位，在 bulk_write_kline(sync_duck_time=False) 后调用"""

    def close(self):
        self.client.close()
```

### Arrow IPC 注意事项

- `open_time` 列类型必须为 `TIMESTAMP`（无时区）。pandas DataFrame 写入前需执行 `tz_localize(None)` 去除时区信息。
- 可能全为 `None` 的 varchar 列（如 `contract_type`）需在写入前强制转换为 `str`，避免 Arrow 推断为 `Null` 类型。
- DoPut 单次不超过 ~500 MB，超过时分 chunk 调用 `bulk_write_kline`。

---

## 5. Schema 设计规范

### 核心约定

| 约定 | 说明 |
|------|------|
| 业务表 | 放 `data` schema（`data.my_kline_5m`） |
| staging 表 | 放 `main` schema（`_staging_my_kline_5m`），无 PK |
| watermark 表 | `data.watermark`，每张业务表对应一行 |
| 通用元数据 | `data.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)` |
| 所有 DDL | 使用 `CREATE TABLE IF NOT EXISTS`，幂等 |
| `CREATE SCHEMA` | 必须与首张表建立在**同一事务**中 |

### watermark 表定义

```sql
CREATE TABLE IF NOT EXISTS data.watermark (
    table_name  VARCHAR PRIMARY KEY,   -- 业务表名，如 'usdt_perp_5m'
    time_column VARCHAR NOT NULL,      -- 时间索引列名，如 'open_time'
    start_time  TIMESTAMP,             -- 数据起始时间（来自 START_DATE 配置）
    duck_time   TIMESTAMP,             -- 最新写入行的时间戳（水位线）
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

- `start_time`：在 `init_schema` 时由配置的 `START_DATE` 写入，后续仅追加 `ON CONFLICT DO NOTHING`，不会被覆盖。
- `duck_time`：每次成功写入后通过 upsert 更新，`start_time` 在 DO UPDATE 中不参与，保持初始值。
- `updated_at`：每次 duck_time 变更时由 `CURRENT_TIMESTAMP` 自动刷新，可用于监控水位推进是否停滞。

### 建表顺序（init_schema 内部）

```python
def init_schema(self, markets, interval, data_sources, start_date=None):
    s = self.schema
    start_ts = f"'{start_date} 00:00:00'" if start_date else "NULL"
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"CREATE TABLE IF NOT EXISTS {s}.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)",
        f"CREATE TABLE IF NOT EXISTS {s}.watermark ("
        f"table_name VARCHAR PRIMARY KEY, time_column VARCHAR NOT NULL, "
        f"start_time TIMESTAMP, duck_time TIMESTAMP, "
        f"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    ]
    for market in markets:
        if market not in data_sources:
            continue
        target = f"{market}_{interval}"
        stmts += [
            f"CREATE TABLE IF NOT EXISTS {s}.{target} (..., PRIMARY KEY (open_time, symbol))",
            f"CREATE TABLE IF NOT EXISTS _staging_{target} (...)",
            # start_time 只在首次创建时写入，ON CONFLICT DO NOTHING 保证幂等
            f"INSERT INTO {s}.watermark (table_name, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{target}', 'open_time', {start_ts}, NULL, CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO NOTHING",
        ]
    self.execute_transaction(stmts)
```

`CREATE SCHEMA IF NOT EXISTS` 和 `CREATE TABLE` 必须放同一事务，否则下一条语句看不到新建的 schema。

### 表命名约定

- 业务表：`{market}_{interval}`（如 `usdt_perp_5m`）
- staging 表：`_staging_{market}_{interval}`（下划线前缀，对 Airport 客户端不可见）
- watermark：`data.watermark`（每张业务表一行）
- 通用元数据：`data.config_dict`（kline_interval 一致性校验等）

---

## 6. 数据写入流程（staging 模式）

DuckDB Appender 不支持 `ON CONFLICT`，因此所有写入都经过 staging 表中转：

```
TRUNCATE _staging
    ↓
DoPut → _staging（无 PK，纯追加）
    ↓
execute_transaction [
    INSERT INTO data.target SELECT * FROM _staging ON CONFLICT DO NOTHING,
    INSERT OR REPLACE INTO data.config_dict VALUES ('{market}_duck_time', '{ts}'),
    TRUNCATE _staging,
]
```

**实时写入**（`write_kline`）：三步在一次调用内完成，watermark 随数据原子更新。

**批量写入**（`bulk_write_kline`）：按 200K 行分块；全部文件写完后统一调用 `replace_duck_time` 更新 watermark。分块期间 `sync_duck_time=False`，避免中间状态下 duck_time 被提前推进。

```python
# 批量写入正确姿势
for fp in parquet_files:
    arrow = load_parquet(fp)
    client.bulk_write_kline(arrow, market, interval, "", sync_duck_time=False)
    # 记录 new_max = max(file_max)

client.replace_duck_time(market, new_max.strftime("%Y-%m-%d %H:%M:%S"))
```

---

## 7. 主入口（main.py）

```python
import os, sys, signal
os.environ['TZ'] = 'UTC'   # 强制 UTC，避免时区隐患

def main():
    from my_ingestor.config import DUCKPORT_ADDR, DUCKPORT_SCHEMA, DATA_SOURCES, ENABLE_WS
    from my_ingestor.duckport_client import DuckportClient
    from my_ingestor.data_jobs import RestDataJobs, WsDataJobs

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)

    # 信号处理：systemd 发 SIGTERM 时优雅退出
    def _exit(sig, frame):
        client.close()
        sys.exit(0)
    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    # 连通性检查：失败立刻退出，由 systemd Restart=on-failure 重试
    try:
        info = client.ping()
    except Exception as e:
        print(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {e}", file=sys.stderr)
        sys.exit(1)

    # 幂等建表
    client.init_schema(...)

    # 启动数据采集（daemon 线程）
    if ENABLE_WS:
        WsDataJobs(client)
    else:
        RestDataJobs(client)

    # 主线程等待信号（daemon 线程随主线程退出）
    try:
        signal.pause()
    except AttributeError:
        import threading
        threading.Event().wait()
```

**必须遵守**：

- `SIGTERM` 和 `SIGINT` 均须调用 `client.close()` 后退出，让 systemd 认为进程正常结束。
- `ping()` 失败时 `sys.exit(1)`，触发 `Restart=on-failure` 让 systemd 等 server 就绪。
- `init_schema()` 每次启动都调用，使用 `IF NOT EXISTS` 保证幂等。
- 数据采集跑在 daemon 线程中，主线程仅做信号等待。

---

## 8. 历史数据入口（loadhist.py）

`loadhist` 是**独立脚本**，与主进程互斥，不可同时运行（共享 staging 表）。

### 标准三步流程

```
Step 0  ping + init_schema（幂等）
Step 1  Download — 从数据源下载文件 → 转换为 Parquet
Step 2  Clean    — 清洗 Parquet（去零量 symbol，trim 首尾无效 K 线）
Step 3  Load     — 读 Parquet → Arrow → bulk_write_kline → replace_duck_time
```

### 增量设计

- 使用 `duck_time - BOUNDARY_DAYS`（默认 1 天）作为 loadhist 的扫描边界：
  - `file_max < boundary` → 跳过（全部已在热库）
  - `file_max >= boundary` → `WHERE open_time >= boundary` 过滤后写入
- `ON CONFLICT DO NOTHING` 保证重复执行幂等。
- 清洗步骤使用 `.ok` 标记文件（记录 mtime）跳过未变化的 Parquet，避免重复清洗。

### main() 结构

```python
def main():
    client = DuckportClient(DUCKPORT_ADDR, DUCKPORT_SCHEMA)
    client.ping()
    init_remote_schema(client)   # Step 0

    run_download(KLINE_INTERVAL)  # Step 1

    markets = get_enabled_markets()
    clean_markets(markets)        # Step 2
    save_to_duckport(client, markets)  # Step 3

    client.close()
    # 退出后打印 Parquet 目录路径，提示用户手动清理
```

---

## 9. 水位线（Watermark）管理

watermark 统一存储在 `data.watermark` 表中，每张业务表对应一行：

| 列 | 类型 | 含义 |
|----|------|------|
| `table_name` | VARCHAR PK | 业务表名，如 `usdt_perp_5m` |
| `time_column` | VARCHAR | 时间索引列名，如 `open_time` |
| `start_time` | TIMESTAMP | 数据起始时间（来自 `START_DATE` 配置，初始化时写入） |
| `duck_time` | TIMESTAMP | 最新成功写入行的时间戳（水位线），NULL 表示尚未写入任何数据 |
| `updated_at` | TIMESTAMP | 水位线最近一次更新时间 |

> `data.config_dict` 仍保留用于 kline_interval 一致性校验等通用元数据，不用于 duck_time 跟踪。

**duck_time 语义**：duck_time 之前的数据已完整写入，ingestor 启动时从 duck_time 断点续传。

**写入时机**：
- 实时写入（`write_kline`）：在同一事务中 upsert watermark，与数据原子提交。
- 批量写入（`loadhist`）：全部 chunk 写完后调用 `replace_duck_time` 统一提交一次，中间过程 `sync_duck_time=False`。

**upsert 模式**：
```sql
INSERT INTO data.watermark (table_name, time_column, start_time, duck_time, updated_at)
VALUES ('usdt_perp_5m', 'open_time', NULL, '2026-04-28 08:00:00', CURRENT_TIMESTAMP)
ON CONFLICT (table_name) DO UPDATE SET
    duck_time  = excluded.duck_time,
    updated_at = CURRENT_TIMESTAMP
-- start_time 不在 DO UPDATE 中，保留初始值不被覆盖
```

**读取时机**：
- 主进程启动时通过 `read_duck_times(markets)` 批量读取，初始化内存 `duck_time` 字典。
- `duckport status` 直接读取 `data.watermark` 全表，无需预知市场列表。

**DuckportClient 接口约定**：
```python
# 构造时传入 interval，read_duck_time 据此拼接 table_name
client = DuckportClient(addr=DUCKPORT_ADDR, schema="data", interval=KLINE_INTERVAL)

client.read_duck_time("usdt_perp")      # 查 watermark WHERE table_name = 'usdt_perp_5m'
client.replace_duck_time("usdt_perp", "2026-04-28 08:00:00")  # upsert duck_time
```

---

## 10. 错误处理与幂等

### 幂等写入

- staging 模式下 `ON CONFLICT DO NOTHING` 保证同一批数据重复写入无副作用。
- `CREATE TABLE IF NOT EXISTS` 保证 DDL 幂等。
- `INSERT OR REPLACE INTO config_dict` 保证 watermark 幂等更新。

### 重试策略

- `execute_transaction` 任意语句失败 → 整个事务 ROLLBACK，不产生中间状态；客户端捕获 `FlightError` 后重试整批。
- REST 拉取失败建议指数退避重试（最多 `retry_times` 次）。
- WebSocket 断连后捕获异常、`gc.collect()` 清理内存，等待固定秒数后重建连接。

### 严重错误处理

- `ping()` 失败 → `sys.exit(1)`，由 systemd 负责重启。
- duck_time 长时间未推进（gap > 499 根 K 线）→ 记录 `ERROR` 级日志并跳过，提示重启，不要强行追历史。
- DoPut schema 不匹配 → 捕获异常、记录错误、丢弃当批数据继续下一周期。

---

## 11. 注册到 registry.json

完成开发并推送到 GitHub 后，向 `duckport-rs/ingestor/registry.json` 追加一条记录：

```json
{
  "my-ingestor": {
    "repo": "https://github.com/your-org/duckport-my-ingestor",
    "description": "My exchange K-line ingestor for duckport",
    "exec": "my-ingestor",
    "default_instance": "my-5m"
  }
}
```

| 字段 | 说明 |
|------|------|
| key（插件名） | 连字符命名，与仓库名后缀一致 |
| `repo` | git clone URL，支持 `git+https://` 格式 |
| `exec` | `pyproject.toml [project.scripts]` 中定义的脚本名 |
| `default_instance` | `duckport install` 交互时的默认实例名建议值 |

注册后用户即可通过 `duckport install my-ingestor` 安装。

---

## 12. 本地开发与调试

### editable 安装

```bash
cd duckport-my-ingestor
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e .
```

`duckport install` 会自动检测本地 checkout（`$(dirname $REPO_DIR)/duckport-<plugin>/pyproject.toml`），存在时以 editable 模式安装，省去每次改代码后重新 `pip install`。

### 启动顺序

```bash
duckport start              # 先启动 duckport-server

# 本地直接跑（非 systemd）
INGESTOR_ENV_FILE=./config.env my-ingestor

# 或通过 systemd
duckport start my-5m
```

### config.env 模板

```bash
# 本地开发时使用，不提交到 git
KLINE_INTERVAL=5m
DATA_SOURCES=usdt_perp
CONCURRENCY=1
START_DATE=2026-01-01
PROXY_URL=
```

### 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| `Invalid column type Null` | 某列全为 None，Arrow 推断为 Null 类型 | 写入前 `df[col] = df[col].astype(str)` |
| `kline interval mismatch` | config.env 中 `KLINE_INTERVAL` 与 DB 已记录值不同 | 确认新旧实例使用同一周期，或清空 config_dict 中的 `kline_interval` 行 |
| `Cannot reach duckport-rs` | duckport-server 未启动 | `duckport start`，确认 :50051 可达 |
| loadhist 与 ingestor 并发 | 共享 staging 表导致数据乱序 | `duckport loadhist` 会先 stop ingestor；手动执行时确保先 stop |
