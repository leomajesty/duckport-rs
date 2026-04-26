# duckport-rs 生产环境部署指南

> DuckDB gRPC 数据库服务 — 通过 Arrow Flight 协议实现多进程读写

---

## 目录

1. [架构概览](#1-架构概览)
2. [环境要求](#2-环境要求)
3. [构建 duckport-server](#3-构建-duckport-server)
4. [服务端配置](#4-服务端配置)
5. [数据目录规划](#5-数据目录规划)
6. [部署 duckport-server](#6-部署-duckport-server)
7. [部署 binance-ingestor](#7-部署-binance-ingestor)
8. [部署 consumer-client](#8-部署-consumer-client)
9. [历史数据导入 (loadhist)](#9-历史数据导入-loadhist)
10. [进程管理 (systemd)](#10-进程管理-systemd)
11. [日志与监控](#11-日志与监控)
12. [备份与恢复](#12-备份与恢复)
13. [升级流程](#13-升级流程)
14. [常见问题排查](#14-常见问题排查)

---

## 1. 架构概览

```
┌──────────────────┐     gRPC :50051      ┌──────────────────────┐
│ binance-ingestor │ ──── DoAction/DoPut ──▶│                      │
│  (Python)        │                       │   duckport-server    │
└──────────────────┘                       │      (Rust)          │
                                           │                      │
┌──────────────────┐     gRPC :50051      │  ┌──────────────┐    │
│ consumer-client  │ ──── DoGet ──────────▶│  │   DuckDB     │    │
│  (Python)        │                       │  │  (embedded)  │    │
└──────────────────┘                       │  └──────────────┘    │
                                           │                      │
┌──────────────────┐     Airport ATTACH    │  read pool (r2d2)   │
│ DuckDB client    │ ──── DoGet ──────────▶│  + single writer    │
│  (Airport ext)   │                       └──────────────────────┘
└──────────────────┘
```

| 组件 | 语言 | 职责 |
|------|------|------|
| **duckport-server** | Rust | 核心数据库服务，管理 DuckDB 实例，提供 Arrow Flight gRPC 接口 |
| **binance-ingestor** | Python | 从 Binance 采集 K 线数据，通过 gRPC 写入 duckport-server |
| **consumer-client** | Python | 消费方查询客户端，封装 DoGet 查询接口 |

---

## 2. 环境要求

### 2.1 服务器

| 项目 | 最低要求 | 推荐 |
|------|---------|------|
| OS | Linux x86_64 / aarch64 | Ubuntu 22.04+ / Debian 12+ |
| CPU | 2 核 | 4 核 |
| 内存 | 2 GB | 8 GB（DuckDB 内存取决于数据量） |
| 磁盘 | 20 GB SSD | 100 GB+ SSD（视历史数据保留周期） |

### 2.2 编译工具链

```bash
# Rust (MSRV 1.85.1)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default 1.85.1

# 验证
rustc --version   # >= 1.85.1
cargo --version
```

### 2.3 Python 运行时

```bash
# Python >= 3.11 (ingestor)  /  >= 3.10 (consumer-client)
python3 --version

# 推荐使用 uv 管理虚拟环境
pip install uv
```

---

## 3. 构建 duckport-server

### 3.1 Release 编译

```bash
cd duckport-rs
cargo build --release --bin duckport-server
```

产物路径：`target/release/duckport-server`

> **注意**：首次编译会从源码构建 DuckDB 1.5.1（bundled 模式），耗时约 5–10 分钟。后续增量编译很快。

### 3.2 交叉编译（可选）

如需在 macOS 上为 Linux 构建：

```bash
# 安装 cargo-zigbuild + zig
cargo install cargo-zigbuild
brew install zig

# 编译 Linux x86_64
cargo zigbuild --release --bin duckport-server --target x86_64-unknown-linux-gnu
```

### 3.3 部署二进制

```bash
# 复制到目标服务器
scp target/release/duckport-server user@prod-server:/opt/duckport/bin/

# 设置可执行权限
ssh user@prod-server 'chmod +x /opt/duckport/bin/duckport-server'
```

---

## 4. 服务端配置

duckport-server 通过**环境变量**配置，所有变量以 `DUCKPORT_` 为前缀。

### 4.1 环境变量一览

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DUCKPORT_DB_PATH` | `./duckport.db` | DuckDB 数据库文件路径（`:memory:` 为纯内存模式） |
| `DUCKPORT_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC 监听地址 |
| `DUCKPORT_ADVERTISED_ADDR` | *(空)* | FlightEndpoint 返回的公开地址，空则使用 listen 地址 |
| `DUCKPORT_CATALOG_NAME` | `duckport` | Airport catalog 名称（客户端 `ATTACH ... AS <name>` 使用） |
| `DUCKPORT_READ_POOL_SIZE` | `4` | 读连接池大小（最小 1） |
| `DUCKPORT_DUCKDB_THREADS` | `0` | DuckDB 工作线程数（0 = DuckDB 自动检测） |
| `DUCKPORT_DUCKDB_MEMORY_LIMIT` | *(空)* | DuckDB 内存上限（如 `4GB`，空 = 不限制） |
| `DUCKPORT_RETENTION_ENABLED` | `false` | **Plan A 默认关闭**：不启动周期 `COPY+DELETE`；设为 `true` 启用旧版热数据归档 |
| `DUCKPORT_RETENTION_TABLE` | `data.retention_tasks` | 周期任务行所在表（由 ingestor 在 `RETENTION_ENABLED=true` 时写入） |
| `RUST_LOG` | `info` | 日志级别（`debug`, `info`, `warn`, `error`） |

### 4.2 生产环境推荐配置

创建 `/opt/duckport/server.env`：

```bash
DUCKPORT_DB_PATH=/data/duckport/duckport.db
DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
DUCKPORT_CATALOG_NAME=duckport
DUCKPORT_READ_POOL_SIZE=4
DUCKPORT_DUCKDB_THREADS=0
DUCKPORT_DUCKDB_MEMORY_LIMIT=4GB
# Plan A: 不开启周期导出+删除，热数据仅保留在 duckport.db
# DUCKPORT_RETENTION_ENABLED=true
RUST_LOG=info
```

> **选型建议**：
> - `READ_POOL_SIZE` 设为 CPU 核数的 50–100%，一般 2–8 足够
> - `DUCKDB_MEMORY_LIMIT` 设为物理内存的 50–70%，避免 OOM
> - 数据库文件应放在 SSD 上，避免 NFS/网络文件系统

---

## 5. 数据目录规划

```
/data/duckport/
├── duckport.db              # DuckDB 主数据库文件
├── duckport.db.wal          # DuckDB WAL 日志（自动生成）
└── pqt/                     # Parquet 归档目录
    ├── usdt_perp_5m/        # USDT 永续合约 5 分钟 K 线
    │   ├── usdt_perp_2026-01.parquet
    │   └── usdt_perp_2026-02.parquet
    └── usdt_spot_5m/        # USDT 现货 5 分钟 K 线
        └── ...

/data/duckport/hist/         # loadhist 历史数据下载临时目录
```

创建目录：

```bash
sudo mkdir -p /data/duckport/pqt
sudo mkdir -p /data/duckport/hist
sudo chown -R duckport:duckport /data/duckport
```

> **重要**：`PARQUET_DIR` 供 loadhist、以及可选的「旧版」周期 retention（`DUCKPORT_RETENTION_ENABLED=true` 且 `RETENTION_ENABLED=true`）使用。**Plan A** 下默认不跑周期 retention，但历史导入/备份仍可能写入 Parquet，目录需与数据规划一致、权限正确。

---

## 6. 部署 duckport-server

### 6.1 手动启动（调试用）

```bash
export DUCKPORT_DB_PATH=/data/duckport/duckport.db
export DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
export DUCKPORT_READ_POOL_SIZE=4
export DUCKPORT_DUCKDB_MEMORY_LIMIT=4GB
export RUST_LOG=info

/opt/duckport/bin/duckport-server
```

启动成功日志：

```
INFO duckport starting db_path="/data/duckport/duckport.db" listen=0.0.0.0:50051 catalog="duckport" read_pool_size=4
INFO duckport Flight service ready (airport read plane + duckport.* write plane) advertised="0.0.0.0:50051"
```

### 6.2 验证连通性

```bash
# 使用 grpcurl 检查 Flight 服务
grpcurl -plaintext localhost:50051 list

# 或使用 Python 快速验证
python3 -c "
import pyarrow.flight as flight
client = flight.connect('grpc://localhost:50051')
for action in client.list_actions():
    print(action.type)
"
```

---

## 7. 部署 binance-ingestor

### 7.1 安装

```bash
cd duckport-rs/ingestor

# 方式一：uv（推荐）
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -e .

# 方式二：pip
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 7.2 配置

创建 `/opt/duckport/ingestor/config.env`：

```bash
# duckport-rs 服务地址
DUCKPORT_ADDR=localhost:50051
DUCKPORT_SCHEMA=data

# K 线周期
KLINE_INTERVAL=5m

# Parquet 归档目录（loadhist / 可选旧版周期 retention 使用）
PARQUET_DIR=/data/duckport/pqt

# 启用的数据源
DATA_SOURCES=usdt_perp,usdt_spot

# 并发度
CONCURRENCY=2

# Plan A: 不注册 data.retention_tasks。若需热库周期导出+删除，设 true 且服务端 DUCKPORT_RETENTION_ENABLED=true
RETENTION_ENABLED=false

# 仅当 RETENTION_ENABLED=true 时与周期任务一起使用
RETENTION_DAYS=7

# 历史数据起始日期
START_DATE=2024-01-01

# Binance API 代理（留空直连）
PROXY_URL=

# WebSocket 模式（false=REST 轮询，true=WebSocket 实时推送）
ENABLE_WS=false
```

### 7.3 启动

```bash
# 指定配置文件
export INGESTOR_ENV_FILE=/opt/duckport/ingestor/config.env

# 方式一：CLI 入口
binance-ingestor

# 方式二：模块入口
python -m binance_ingestor.main

# 方式三：便捷脚本
python start_ingestor.py
```

### 7.4 启动顺序

1. **必须先启动 duckport-server** — ingestor 启动时会执行 `ping` 检测 server 可达性
2. ingestor 启动后自动执行：
   - `init_schema`：确保远端 DuckDB 存在所需的 schema 和表
   - `init_history_data`：从 `duck_time` 水位线补齐缺失的 K 线数据
   - 进入周期采集循环（REST 5 分钟轮询 / WebSocket 实时推送）
   - **Plan A**：不注册周期 retention；**旧版**需同时设 `RETENTION_ENABLED=true` 与 `DUCKPORT_RETENTION_ENABLED=true`，由 duckport-server 侧按 8h 周期执行 `COPY+DELETE`（见 `retention.rs`）

---

## 8. 部署 consumer-client

### 8.1 安装

```bash
cd duckport-rs/client

uv venv --python 3.10 .venv
source .venv/bin/activate
uv pip install -e .
```

### 8.2 使用方式

```python
from duckport_consumer import DuckportConsumer

consumer = DuckportConsumer(addr="localhost:50051", schema="data")

# 查询 K 线数据
table = consumer.get_market("usdt_perp", interval="5m", limit=1000)
df = table.to_pandas()

# 执行自定义 SQL
table = consumer.query("SELECT * FROM data.usdt_perp_5m WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 100")

# 获取最新水位线
duck_time = consumer.read_duck_time("usdt_perp")
```

### 8.3 Airport 扩展直连（可选）

DuckDB 客户端安装 Airport 扩展后可直接查询：

```sql
INSTALL airport FROM community;
LOAD airport;

ATTACH 'grpc://prod-server:50051' AS dp (TYPE AIRPORT);
SELECT * FROM dp.data.usdt_perp_5m WHERE symbol = 'BTCUSDT' LIMIT 10;
```

---

## 9. 历史数据导入 (loadhist)

首次部署或需要重建历史数据时执行。

> **⚠️ 互斥约束**：loadhist 和 binance-ingestor **不可同时运行**。两者共享同一组 staging 表（如 `_staging_usdt_perp_5m`），并发执行会因三步非原子 RPC 序列（TRUNCATE → DoPut → INSERT INTO target）交叉调度而导致数据丢失。必须先停止 ingestor，完成 loadhist 后再启动。

### 9.1 配置

确保 `config.env` 中包含以下额外配置：

```bash
# 历史数据下载临时目录
RESOURCE_PATH=/data/duckport/hist
```

### 9.2 执行

```bash
# 1. 必须先停止 ingestor（避免 staging 表争用）
sudo systemctl stop binance-ingestor

# 2. 执行历史导入
export INGESTOR_ENV_FILE=/opt/duckport/ingestor/config.env
loadhist
# 或: python -m binance_ingestor.loadhist

# 3. 导入完成后启动 ingestor
sudo systemctl start binance-ingestor
```

### 9.3 执行流程

1. **Step 0** — 通过 RPC 在 duckport-rs 上初始化 schema 和表
2. **Step 1** — 从 Binance 公开数据集下载 zip 文件并转换为 Parquet
3. **Step 2** — DuckDB 内存模式分析本地 Parquet 并清理异常数据
4. **Step 3** — 读取 Parquet → Arrow → `bulk_write_kline()` 分块写入 duckport-rs

> **耗时**：取决于历史数据量和网络带宽，首次全量导入可能需要数小时。
> `bulk_write_kline` 按 200K 行分块写入，支持中断后幂等重跑（ON CONFLICT DO NOTHING）。

---

## 10. 进程管理 (systemd)

### 10.1 duckport-server.service

创建 `/etc/systemd/system/duckport-server.service`：

```ini
[Unit]
Description=duckport-rs gRPC Database Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=duckport
Group=duckport
WorkingDirectory=/opt/duckport
EnvironmentFile=/opt/duckport/server.env
ExecStart=/opt/duckport/bin/duckport-server
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/data/duckport
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

### 10.2 binance-ingestor.service

创建 `/etc/systemd/system/binance-ingestor.service`：

```ini
[Unit]
Description=Binance Data Ingestor for duckport-rs
After=duckport-server.service
Requires=duckport-server.service

[Service]
Type=simple
User=duckport
Group=duckport
WorkingDirectory=/opt/duckport/ingestor
Environment=INGESTOR_ENV_FILE=/opt/duckport/ingestor/config.env
ExecStart=/opt/duckport/ingestor/.venv/bin/binance-ingestor
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 10.3 启用并启动

```bash
# 创建服务用户
sudo useradd -r -s /sbin/nologin -d /opt/duckport duckport
sudo chown -R duckport:duckport /opt/duckport /data/duckport

# 加载 & 启动
sudo systemctl daemon-reload
sudo systemctl enable duckport-server binance-ingestor
sudo systemctl start duckport-server

# 等 server 就绪后启动 ingestor
sleep 3
sudo systemctl start binance-ingestor

# 查看状态
sudo systemctl status duckport-server binance-ingestor

# 查看日志
sudo journalctl -u duckport-server -f
sudo journalctl -u binance-ingestor -f
```

---

## 11. 日志与监控

### 11.1 日志配置

**duckport-server** 使用 `tracing-subscriber`，通过 `RUST_LOG` 环境变量控制：

```bash
# 仅看 duckport 相关日志
RUST_LOG=duckport_server=info

# 包含 airport 库日志
RUST_LOG=info,airport=debug

# 调试模式
RUST_LOG=debug
```

**binance-ingestor** 使用 Python `logging`，输出到 stdout/stderr。

### 11.2 健康检查

```bash
# Python 脚本检查
python3 -c "
import pyarrow.flight as flight
import json, sys
client = flight.connect('grpc://localhost:50051')
result = list(client.do_action(flight.Action('duckport.ping', b'{}')))
info = json.loads(result[0].body.to_pybytes())
print(f'server={info[\"server\"]}, duckdb={info[\"duckdb_version\"]}')
sys.exit(0)
" || echo "HEALTH CHECK FAILED"
```

可将以上脚本配置为 cron 或监控探针。

### 11.3 关键指标观测点

当前版本尚未内置 Prometheus 端点（Phase 3 计划中），但可通过以下方式间接监控：

| 指标 | 观测方式 |
|------|---------|
| 进程存活 | systemd watchdog / `pgrep duckport-server` |
| 端口可达 | `nc -z localhost 50051` |
| 数据新鲜度 | 查询 `config_dict` 中的 `duck_time`，与当前时间比较 |
| 磁盘使用 | `du -sh /data/duckport/` |
| DuckDB 文件大小 | `ls -lh /data/duckport/duckport.db` |

### 11.4 水位线监控脚本

```bash
#!/bin/bash
# check_freshness.sh — 检查数据新鲜度
python3 -c "
from duckport_consumer import DuckportConsumer
from datetime import datetime, timezone, timedelta

c = DuckportConsumer(addr='localhost:50051', schema='data')
for market in ['usdt_perp', 'usdt_spot']:
    dt = c.read_duck_time(market)
    if dt is None:
        print(f'WARN: {market} duck_time is None')
        continue
    age = datetime.now(timezone.utc) - dt
    status = 'OK' if age < timedelta(minutes=10) else 'STALE'
    print(f'{status}: {market} duck_time={dt}, age={age}')
"
```

---

## 12. 备份与恢复

### 12.1 数据备份策略

| 数据 | 备份方式 | 频率 |
|------|---------|------|
| `duckport.db` | 文件级快照（cp / LVM snapshot / ZFS snapshot） | 每日 |
| `pqt/` Parquet 文件 | rsync / 对象存储同步 | 每日增量 |
| `config.env` 等配置 | 纳入版本控制 | 每次变更 |

### 12.2 备份脚本

```bash
#!/bin/bash
# backup_duckport.sh
BACKUP_DIR=/backup/duckport/$(date +%Y%m%d)
mkdir -p "$BACKUP_DIR"

# 停止 ingestor 避免写入冲突
sudo systemctl stop binance-ingestor

# DuckDB checkpoint（flush WAL）
python3 -c "
import pyarrow.flight as flight
client = flight.connect('grpc://localhost:50051')
client.do_action(flight.Action('duckport.execute', b'{\"sql\": \"CHECKPOINT\"}'))
print('checkpoint done')
"

# 复制数据库文件
cp /data/duckport/duckport.db "$BACKUP_DIR/"

# 同步 Parquet 归档
rsync -av /data/duckport/pqt/ "$BACKUP_DIR/pqt/"

# 恢复 ingestor
sudo systemctl start binance-ingestor

echo "Backup completed: $BACKUP_DIR"
```

### 12.3 恢复流程

```bash
# 1. 停止所有服务
sudo systemctl stop binance-ingestor duckport-server

# 2. 替换数据库文件
cp /backup/duckport/20260422/duckport.db /data/duckport/duckport.db
rsync -av /backup/duckport/20260422/pqt/ /data/duckport/pqt/

# 3. 删除 WAL（如有残留）
rm -f /data/duckport/duckport.db.wal

# 4. 重启服务
sudo systemctl start duckport-server
sleep 3
sudo systemctl start binance-ingestor
```

---

## 13. 升级流程

### 13.1 duckport-server 升级

```bash
# 1. 编译新版本
cd duckport-rs && git pull && cargo build --release --bin duckport-server

# 2. 停止服务
sudo systemctl stop binance-ingestor
sudo systemctl stop duckport-server

# 3. 替换二进制
sudo cp target/release/duckport-server /opt/duckport/bin/duckport-server

# 4. 重启
sudo systemctl start duckport-server
sleep 3
sudo systemctl start binance-ingestor

# 5. 验证
sudo systemctl status duckport-server binance-ingestor
```

### 13.2 binance-ingestor 升级

```bash
# 1. 更新代码
cd duckport-rs/ingestor && git pull

# 2. 更新依赖
source .venv/bin/activate
pip install -e .

# 3. 重启
sudo systemctl restart binance-ingestor
```

> **注意**：DuckDB 版本已锁定为 1.5.1（Rust crate `=1.10501.0`）。升级 DuckDB 版本需同步更新 `Cargo.toml` 中的 pin 并重新编译，且需评估数据库文件兼容性。

---

## 14. 常见问题排查

### Q1: ingestor 启动失败 "Cannot reach duckport-rs"

```
ERROR Cannot reach duckport-rs at localhost:50051: ...
```

**原因**：duckport-server 未启动或地址配置错误。
**解决**：确认 server 已运行 → 检查 `DUCKPORT_ADDR` → 检查防火墙端口 50051。

### Q2: "append rolled back: Invalid column type Null"

```
append rolled back: append_record_batch #0: Invalid column type Null, name: Field { "contract_type": nullable Null }
```

**原因**：Pandas DataFrame 中某列全为 None，Arrow 推断为 Null 类型，与 DuckDB 目标表的 VARCHAR 不兼容。
**解决**：在写入前对可能全 None 的列做显式类型转换：

```python
for col in ['contract_type', 'margin_asset']:
    if col in df.columns:
        df[col] = df[col].astype(str).replace('None', '')
```

### Q3: "Expected bytes, got a 'decimal.Decimal' object"

**原因**：Binance API 返回的某些字段为 `decimal.Decimal` 类型，Arrow 无法自动转换。
**解决**：写入前将 Decimal 列转为 str：

```python
df['price_tick'] = df['price_tick'].astype(str)
```

### Q4: DuckDB 数据库文件持续增长

**原因**：WAL 未及时 checkpoint。
**解决**：

```bash
# 手动触发 checkpoint
python3 -c "
import pyarrow.flight as flight
client = flight.connect('grpc://localhost:50051')
client.do_action(flight.Action('duckport.execute', b'{\"sql\": \"CHECKPOINT\"}'))
"
```

或设置定时 cron：

```cron
0 */4 * * * /opt/duckport/scripts/checkpoint.sh
```

### Q5: 内存占用过高

**原因**：DuckDB 默认不限制内存，大查询可能占满 RAM。
**解决**：设置 `DUCKPORT_DUCKDB_MEMORY_LIMIT=4GB`（根据实际内存调整）。

### Q6: 多实例部署注意事项

DuckDB 单写模型意味着**同一个数据库文件只能被一个 duckport-server 进程打开**。如需多实例：
- 使用不同的 `DUCKPORT_DB_PATH` 指向不同的数据库文件
- 或使用负载均衡器将写请求路由到主实例，读请求分发到只读副本（需自行实现同步机制）

---

## 附录 A — 环境变量速查

### duckport-server

| 变量 | 默认 | 必填 |
|------|------|------|
| `DUCKPORT_DB_PATH` | `./duckport.db` | 否 |
| `DUCKPORT_LISTEN_ADDR` | `0.0.0.0:50051` | 否 |
| `DUCKPORT_ADVERTISED_ADDR` | *(空)* | 否 |
| `DUCKPORT_CATALOG_NAME` | `duckport` | 否 |
| `DUCKPORT_READ_POOL_SIZE` | `4` | 否 |
| `DUCKPORT_DUCKDB_THREADS` | `0` | 否 |
| `DUCKPORT_DUCKDB_MEMORY_LIMIT` | *(空)* | 否 |
| `DUCKPORT_RETENTION_ENABLED` | `false` | 否 |
| `DUCKPORT_RETENTION_TABLE` | `data.retention_tasks` | 否 |
| `RUST_LOG` | `info` | 否 |

### binance-ingestor

| 变量 | 默认 | 必填 |
|------|------|------|
| `INGESTOR_ENV_FILE` | `config.env` | 否 |
| `DUCKPORT_ADDR` | `localhost:50051` | 否 |
| `DUCKPORT_SCHEMA` | `data` | 否 |
| `KLINE_INTERVAL` | `5m` | 否 |
| `PARQUET_DIR` | `data/pqt` | 生产必填 |
| `DATA_SOURCES` | `usdt_perp,usdt_spot` | 否 |
| `CONCURRENCY` | `2` | 否 |
| `RETENTION_ENABLED` | `false` | 否 |
| `RETENTION_DAYS` | `7` | 否（仅与旧版周期 retention 同开时有效） |
| `START_DATE` | *(未设则回退极早)* | 生产必填 |
| `PROXY_URL` | *(空)* | 否 |
| `ENABLE_WS` | `false` | 否 |
| `RESOURCE_PATH` | `data/hist` | loadhist 时需要 |

---

## 附录 B — 依赖版本锁定

| 组件 | 版本 |
|------|------|
| DuckDB (Rust crate) | `=1.10501.0` (DuckDB 1.5.1) |
| Arrow / Arrow Flight | 58 |
| tonic (gRPC) | 0.14 |
| prost (protobuf) | 0.14 |
| Rust edition | 2021, MSRV 1.85.1 |
| Python (ingestor) | >= 3.11 |
| Python (consumer) | >= 3.10 |
| PyArrow | >= 15.0 |
| pandas | >= 2.0 |

---

## 附录 C — 端口与网络

| 服务 | 端口 | 协议 | 方向 |
|------|------|------|------|
| duckport-server | 50051 (可配置) | gRPC (HTTP/2) | ingestor → server, consumer → server |
| Binance REST API | 443 | HTTPS | ingestor → 外网 |
| Binance WebSocket | 443 | WSS | ingestor → 外网 (ENABLE_WS=true 时) |

防火墙规则：
- 入站：允许 50051/tcp（仅限内网或可信 IP）
- 出站：允许 443/tcp（ingestor 访问 Binance API）
