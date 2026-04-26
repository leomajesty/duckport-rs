# 从 duckport beta_v2.2 迁移到 duckport-rs（数据迁移手册）

> 适用对象：当前线上运行 `duckport beta_v2.2`（Python FlightServer）并计划切换到 `duckport-rs`（Rust Flight 服务）的环境。

---

## 1. 迁移目标

- 保留并迁移以下核心数据：
  - K 线表：`usdt_perp_<interval>`、`usdt_spot_<interval>`
  - 元数据表：`config_dict`（含各市场的 `duck_time`、`kline_interval` 等；**勿**把 duckport-rs 已废弃的键写入新库，见下文「config_dict 废弃键」）
  - 交易对信息：`exginfo`
- 迁移后由 `duckport-rs + binance-ingestor` 接管持续写入
- 消费查询改连 `duckport-rs` 地址（默认 `50051`）

---

## 2. 迁移策略选择

| 策略 | 推荐度 | 停机时长 | 适用场景 |
|---|---|---|---|
| A. 全量重建（loadhist） | 中 | 长 | 能接受重新下载历史数据 |
| B. DB 文件直迁（推荐） | 高 | 短 | 需要快速切换、保留既有热库数据 |
| C. Parquet 中转导入 | 中 | 中 | 旧 DB 版本兼容风险较高时 |

本文以 **策略 B（推荐）** 给出标准步骤。

---

## 3. 切换前检查清单

1. 旧服务可访问且数据稳定（无持续报错）
2. 已备份旧库文件与 Parquet 目录
3. 新环境已部署 `duckport-rs` 可执行文件
4. 明确切换窗口（建议避开高峰）

---

## 4. 迁移步骤（策略 B）

## Step 0：准备变量

```bash
export OLD_DB_PATH=/data/duckport-beta/duckdb.db
# 旧环境 Parquet 根目录；迁移脚本只读、把数据扫进新 DuckDB，**不必**把文件拷到新目录
export OLD_PQT_DIR=/data/duckport-beta/pqt

export NEW_DB_PATH=/data/duckport-rs/duckport.db
# 新环境 ingestor 的 PARQUET_DIR：可与 OLD_PQT_DIR 相同（共享盘/NFS/原路径）或日后再定，与「数据是否进 DuckDB」无关
export NEW_PARQUET_DIR=/data/duckport-rs/pqt   # 可选，仅作运维规划用

# K 线表名后缀，须与现网一致，例如 5m → 表名 usdt_perp_5m
export MIG_KLINE_INTERVAL=5m

export DUCKPORT_ADDR=127.0.0.1:50051
export DUCKPORT_SCHEMA=data
```

**路径可见性**：执行下面 DuckDB 迁移脚本的机器必须能**直接读**`OLD_PQT_DIR` 下文件（本机盘、NFS、只读挂载均可）。新机器若无旧盘，可先把 `OLD_PQT_DIR` 挂到同一路径或拷到**临时**目录再读入新库；若仅希望数据进热库、不要求保留独立 Parquet 目录，可只在迁移时挂读一次，完成后无需长期保留新目录中的副本。

### config_dict 废弃键（duckport-rs / Plan A）

旧版 beta_v2.2 若曾启用「热表 + Parquet」混合查询，可能在 `config_dict` 中写入 **Parquet 切分水位**，键名形如 `{market}_{interval}_pqt_time`（例如 `usdt_perp_15m_pqt_time`）。**duckport-rs 已不再使用该类键**；迁移时应从旧库 **SELECT 时排除**，不要 `INSERT` 进新库，以免遗留无效配置。

若你曾用旧版脚本全量拷贝过 `config_dict`，可在导入结束后补一行清理：

```sql
DELETE FROM data.config_dict WHERE ends_with(key, '_pqt_time');
```

（`duck_time` 等键名以 `_duck_time` 结尾，不会被上述语句误删。）

## Step 1：停旧服务（冻结写入）

```bash
# 按你的部署方式替换
pkill -f start_server.py || true
```

确保旧版不再写入，避免迁移窗口产生新数据。

## Step 2：备份旧数据

```bash
mkdir -p /backup/duckport-beta
cp "$OLD_DB_PATH" "/backup/duckport-beta/duckdb.db.$(date +%Y%m%d_%H%M%S)"
rsync -a "$OLD_PQT_DIR/" "/backup/duckport-beta/pqt_$(date +%Y%m%d_%H%M%S)/"
```

## Step 3：启动 duckport-rs（空库）并初始化 schema

```bash
export DUCKPORT_DB_PATH="$NEW_DB_PATH"
export DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
./target/release/duckport-server &
sleep 2
```

初始化表结构（通过 ingestor 客户端；`interval` 须与 `MIG_KLINE_INTERVAL` 一致）：

```bash
python3 <<'PYEOF'
import os, sys
sys.path.insert(0, os.path.abspath("ingestor"))
from binance_ingestor.duckport_client import DuckportClient

iv = os.environ.get("MIG_KLINE_INTERVAL", "5m")
client = DuckportClient(addr=os.environ.get("DUCKPORT_ADDR", "127.0.0.1:50051"),
                        schema=os.environ.get("DUCKPORT_SCHEMA", "data"))
client.init_schema(
    markets=["usdt_perp", "usdt_spot"],
    interval=iv,
    data_sources={"usdt_perp", "usdt_spot"},
)
client.verify_kline_interval(iv)
client.close()
print("schema initialized")
PYEOF
```

## Step 4：停止 duckport-rs，执行离线数据导入

```bash
pkill -f duckport-server || true
sleep 1
```

```bash
python3 <<'PYEOF'
import glob
import os

import duckdb

old_db = os.environ["OLD_DB_PATH"]
new_db = os.environ["NEW_DB_PATH"]
kline_interval = os.environ.get("MIG_KLINE_INTERVAL", "5m")

con = duckdb.connect(new_db)
con.execute(f"ATTACH '{old_db}' AS old_db (READ_ONLY)")

# 1) kline（热库）
for mkt in ("usdt_perp", "usdt_spot"):
    table = f"{mkt}_{kline_interval}"
    try:
        con.execute(f"""
            INSERT INTO data.{table}
            SELECT open_time, symbol, open, high, low, close,
                   volume, quote_volume, trade_num,
                   taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
                   avg_price
            FROM old_db.{table}
            ON CONFLICT DO NOTHING
        """)
        cnt = con.execute(f"SELECT count(*) FROM data.{table}").fetchone()[0]
        print(f"{table}: {cnt} rows")
    except Exception as e:
        print(f"{table}: skip ({e})")

# 2) config_dict（排除 duckport-rs 已废弃的 *_pqt_time，勿写入新库）
try:
    con.execute("""
        INSERT INTO data.config_dict (key, value)
        SELECT key, value FROM old_db.config_dict
        WHERE NOT ends_with(key, '_pqt_time')
        ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
    """)
    print("config_dict migrated (skipped keys ending with _pqt_time)")
except Exception as e:
    print(f"config_dict: {e}")

# 3) exginfo
try:
    con.execute("""
        INSERT INTO data.exginfo
        SELECT market, symbol, status, base_asset, quote_asset,
               price_tick, lot_size, min_notional_value,
               contract_type, margin_asset, pre_market, created_time
        FROM old_db.exginfo
        ON CONFLICT DO NOTHING
    """)
    cnt = con.execute("SELECT count(*) FROM data.exginfo").fetchone()[0]
    print(f"exginfo: {cnt} rows")
except Exception as e:
    print(f"exginfo: {e}")

con.execute("DETACH old_db")

# 4) 从**原** Parquet 目录读入新库（不复制 .parquet 文件到新路径）
#    与热库行重复时由主键 (open_time, symbol) ON CONFLICT DO NOTHING 去重
old_pqt = os.environ.get("OLD_PQT_DIR", "").rstrip("/")
if old_pqt:
    for mkt in ("usdt_perp", "usdt_spot"):
        tbl = f"{mkt}_{kline_interval}"
        subdir = os.path.join(old_pqt, tbl)
        pattern = os.path.join(subdir, f"{mkt}_*.parquet")
        files = glob.glob(pattern)
        if not files:
            print(f"parquet: no files for {pattern}, skip")
            continue
        g = pattern.replace("\\", "/").replace("'", "''")
        try:
            con.execute(f"""
                INSERT INTO data.{tbl}
                SELECT open_time, symbol, open, high, low, close,
                       volume, quote_volume, trade_num,
                       taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
                       avg_price
                FROM read_parquet('{g}')
                ON CONFLICT DO NOTHING
            """)
            cnt = con.execute(f"SELECT count(*) FROM data.{tbl}").fetchone()[0]
            print(f"parquet+hot merged {tbl}: {cnt} rows (glob {pattern})")
        except Exception as e:
            print(f"parquet import {tbl}: {e}")
else:
    print("OLD_PQT_DIR empty, skip parquet import")

con.execute("CHECKPOINT")
con.close()
print("migration done")
PYEOF
```

> **说明**  
> - 旧版目录一般为 `{OLD_PQT_DIR}/{market}_{interval}/` 下若干 `{market}_*.parquet`；若你环境文件名不同，可改为单文件列表或 `list_files` 再 `read_parquet([...])`。  
> - 仅当**执行迁移的进程**能访问 `OLD_PQT_DIR` 时才能用本段；在**新机器**上跑脚本时，请把旧 Parquet 挂到本机路径后把 `OLD_PQT_DIR` 指过去。  
> - 目标是不再依赖「把 Parquet 整棵 rsync 到新机的永久目录」；热库中有、Parquet 中也有的行会保留一条。

## Step 5：配置 ingestor 的 `PARQUET_DIR`（无需复制 Parquet 文件）

**Plan A** 下消费与查询不依赖本机 `read_parquet` 混查，但 `loadhist`、可选的周期 retention 仍会用到 `PARQUET_DIR` 所指的目录。

任选其一即可，**无需要求**为迁移专门 `rsync` 一份树到新盘：

- **共享存储**：`PARQUET_DIR` 与旧环境相同（NFS/同一挂载点）  
- **仅同机双目录**：`PARQUET_DIR` 仍指向原路径 `OLD_PQT_DIR`  
- **新目录**：只有当你希望 ingestor 今后写入**另一路径**时，再设 `NEW_PARQUET_DIR` 并 `rsync`；若不需要，**不必**为搬 Parquet 而复制，数据已在 Step 4 进 `duckport.db`  

在 `ingestor/config.env` 中：

```bash
# 示例：与旧机同一目录，不拷文件
PARQUET_DIR=/data/duckport-beta/pqt
```

## Step 6：启动 duckport-rs 与 binance-ingestor

```bash
export DUCKPORT_DB_PATH="$NEW_DB_PATH"
export DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
./target/release/duckport-server &

# ingestor
export INGESTOR_ENV_FILE=./ingestor/config.env
python -m binance_ingestor.main
```

---

## 5. 数据校验（必须执行）

## 5.1 行数校验

```sql
SELECT count(*) FROM data.usdt_perp_5m;
SELECT count(*) FROM data.usdt_spot_5m;
SELECT count(*) FROM data.exginfo;
SELECT count(*) FROM data.config_dict;
```

## 5.2 时间范围校验

```sql
SELECT min(open_time), max(open_time) FROM data.usdt_perp_5m;
SELECT min(open_time), max(open_time) FROM data.usdt_spot_5m;
```

## 5.3 水位线校验

```sql
SELECT * FROM data.config_dict WHERE key LIKE '%duck_time%';
```

## 5.4 废弃键校验（config_dict）

确认不存在已废弃的 Parquet 混合水位键：

```sql
SELECT key, value FROM data.config_dict WHERE ends_with(key, '_pqt_time');
```

期望：**0 行**。若有残留，执行 `DELETE FROM data.config_dict WHERE ends_with(key, '_pqt_time');` 后 `CHECKPOINT`。

校验规则：

- 新库行数应 `>=` 旧库（因为可能有回补）
- `duck_time` 不能为空
- 新库最大时间应接近切换时间
- 新库 `config_dict` 中不应再出现 `*_pqt_time`

---

## 6. 应用切换步骤

1. 先切消费侧读地址到 `duckport-rs`（只读验证）
2. 再切 ingestor 写地址到 `duckport-rs`
3. 观察 1~2 个 K 线周期，确认实时写入正常
4. 下线旧版 `duckport beta_v2.2`

---

## 7. 回滚方案

在切换后若发现问题：

1. 立即停 `duckport-rs` 与新 ingestor
2. 恢复旧服务进程并指回旧 DB
3. 排查后重新执行迁移（旧库备份不动）

---

## 8. 常见问题

### Q1: 旧库 ATTACH 失败（版本兼容）

- 现象：`ATTACH old_db` 报版本/文件格式错误
- 处理：改用策略 C（先从旧库导出 parquet，再导入新库）

### Q2: 为什么要先停新 server 再导入？

- 避免 server 运行中并发占用 DB 文件，降低锁冲突与损坏风险

### Q3: funding/open_interest 如何处理？

- 若 beta_v2.2 在用这两张表，需要先在新库补 DDL，再按同样方式 `INSERT ... ON CONFLICT DO NOTHING`

### Q4: `usdt_perp_*_pqt_time` 一类键还要保留吗？

- **不需要。** 该类键仅服务旧版「热表 + `read_parquet`」混合查询；duckport-rs（Plan A）查询只走热表，ingestor 也不再维护 `*_pqt_time`。迁移时应用 Step 4 中的 `WHERE NOT ends_with(key, '_pqt_time')` 过滤；已误导入的用 5.4 节 `DELETE` 清理即可。

---

## 9. 迁移完成定义（Done Criteria）

- `duckport.ping` 正常
- 核心表行数、时间范围、水位线校验通过
- ingestor 连续写入至少 2 个周期成功
- 消费侧核心查询无回归
- 旧服务已下线并保留可回滚备份
