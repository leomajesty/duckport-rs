# 升级到 duckport-rs 1.1.0

1.1.0 带来三项变化：

- **DuckDB 1.5.1 → 1.5.6**。修复 [duckdb#22279](https://github.com/duckdb/duckdb/issues/22279)：带主键的表在主键冲突导致写入回滚后，后续写入的字符串会混入被回滚的内容并落盘。存储格式不变，不需要转换库文件。
- **新的 retention**。`DUCKPORT_RETENTION_ENABLED` 改为按天滚动删除，规则在 `data.retention_policy`，默认关闭，打开后默认先 dry-run。旧的按月 `COPY+DELETE` 逻辑和 `DUCKPORT_RETENTION_TABLE` 废弃，`data.retention_tasks` 不再使用。
- **业务表不再建主键**。DuckDB 1.5 不回收带索引表删除的行，retention 删了也控制不住磁盘；联合主键也不参与查询，写入反而更慢。插件改用 anti-join 去重写入（原因和实测见 [`ingestor-dev-guide.md` 5.1](./ingestor-dev-guide.md#51-为什么业务表不建主键)）。

只升级服务端、不做后面几步也可以正常运行，行为与 1.0.x 相同。要用 retention 或修复 `exginfo` 膨胀，需要完成全部步骤。

## 升级顺序

必须先升级插件、再迁移表结构：旧插件的 `ON CONFLICT DO NOTHING` 遇到无主键的表会直接报错，而新插件在带主键的旧表上同样能正确写入。

| | 表带主键（迁移前） | 表无主键（迁移后） |
|---|---|---|
| 旧插件（`ON CONFLICT DO NOTHING`） | 正常 | 报错 `There are no UNIQUE/PRIMARY KEY constraints that refer to this table` |
| 新插件（anti-join） | 正常 | 正常 |

### 1. 升级服务端（不停写）

```bash
cd <repo_dir> && git pull --ff-only
sudo env GITHUB_REPO=sjmc-quant/duckport-rs RELEASE_TAG=v1.1.0 bash ./deploy.sh   # 保留现有 server.env
sudo duckport restart server
duckport status            # 版本显示 v1.1.0，并多出 Retention 一节
```

服务端启动时会自动创建 `data.retention_policy` 和 `data.retention_log`。通过 Airport 连接的 DuckDB 客户端，`duckdb` 版本同步升到 1.5.6。

回退：重新部署 v1.0.4。

### 2. 升级插件（逐个实例重启）

```bash
sudo duckport update <实例名>     # 每个实例执行一次
duckport status                   # 确认 watermark 继续推进
```

需要的最低版本：binance-ingestor 0.2.0、binance-funding 0.2.0。新插件启动时会在 `data.config_dict` 登记 `write_mode:<表名> = anti_join`，第 3 步据此判断哪些表可以迁移。

回退：装回旧版本（第 3 步之前）。

### 3. 迁移表结构（停写 5 到 15 分钟）

先做预检，不修改任何东西：

```bash
sudo duckport migrate nopk --plan
```

输出会列出带主键的业务表，标出哪些可以迁移（`ready`）、哪些因为写它的插件还没升级而跳过，并检查磁盘：迁移期间新旧两份数据同时存在，空闲空间需不小于库文件大小。

建议先备份库文件（停服后复制 `duckport.db` 和 `duckport.db.wal`），然后执行：

```bash
sudo duckport migrate nopk
```

它会依次：停止正在运行的 ingestor → 对每张可迁移的表建一张同结构、无约束的新表，按月 `INSERT ... ORDER BY 时间列` 拷贝 → 核对行数 → 在一个事务里把原表改名为 `<表>__pk_old`、新表改为原名 → 删除废弃的 `data.retention_tasks` → CHECKPOINT → 启动 ingestor。

- `exginfo` 一并重建：只拷贝有效行，带主键时累积的已删除行随之消失；`market` 不是普通标识符（超过 32 个字符等）的损坏行会被剔除。
- 只想迁移部分表：`sudo duckport migrate nopk --tables usdt_perp_1m usdt_spot_1m`。
- 耗时参考：测试中约 2000 万行/秒（1.3 亿行 6 秒）。20 亿行的库纯计算约 2 分钟，加上冷读库文件，预计 5 到 15 分钟。

**回退**（第 4 步之前随时可以）：

```bash
sudo duckport migrate nopk --rollback
```

把 `<表>__pk_old` 恢复为原表。有时间列的表，迁移后新写入的数据会补回旧表；`exginfo` 用当前快照替换。新插件在带主键的表上也能正常写入，回退后不需要降级插件。

### 4. 清理旧表（观察 24 小时后）

```bash
sudo duckport migrate nopk --finalize
```

删除所有 `<表>__pk_old` 并 CHECKPOINT。释放的空间留给后续写入复用，库文件本身不会变小；主键索引占用的内存随之释放。

### 5. 开启 retention（按需）

需要全量历史的研究机可以不开。只需要近期数据的机器：

```bash
# server.env：DUCKPORT_RETENTION_ENABLED=true（DUCKPORT_RETENTION_DRY_RUN 默认 true）
sudo duckport restart server
duckport retention set usdt_perp_1m 120
duckport retention set usdt_spot_1m 120
duckport retention dry-run        # 核对截止日和预计删除行数
duckport retention log
# 确认无误后 server.env 改 DUCKPORT_RETENTION_DRY_RUN=false，再重启
```

保留天数至少要覆盖策略的最长回看窗口再加余量（consumer 的 `get_market` / `get_symbol` 不传 `begin` 时默认回看 90 天）。各状态的含义见 [`deploy.md` 4.3](./deploy.md#43-数据保留retention)。

## 在副本上演练迁移

正式迁移前，用库文件副本演练一次，拿到准确耗时。演练不影响线上服务：

```bash
sudo duckport stop all                      # 复制前停服，保证文件一致
cp /data/duckport/duckport.db* /data/duckport-rehearsal/
sudo duckport start all

# 在另一个端口上用副本起一个临时服务端
sudo DUCKPORT_DB_PATH=/data/duckport-rehearsal/duckport.db DUCKPORT_LISTEN_ADDR=127.0.0.1:50061 \
  /opt/duckport/bin/duckport-server &
PY=/opt/duckport/ingestors/<任一实例>/.venv/bin/python
time $PY <repo_dir>/scripts/migrate_nopk.py --addr 127.0.0.1:50061 \
  --db-path /data/duckport-rehearsal/duckport.db migrate
kill %1 && rm -rf /data/duckport-rehearsal
```

## 可选：压缩库文件

删除旧表后，释放的块只在文件内部复用，文件不会变小。需要真正释放磁盘时，停服后复制出一个紧凑的新文件：

```bash
sudo duckport stop all
$PY - <<'EOF'
import duckdb   # 1.5.6
con = duckdb.connect()
con.execute("ATTACH '/data/duckport/duckport.db' AS old (READ_ONLY)")
con.execute("ATTACH '/data/duckport/duckport.compact.db' AS new")
con.execute("COPY FROM DATABASE old TO new")
EOF
mv /data/duckport/duckport.db /data/duckport/duckport.db.before-compact
mv /data/duckport/duckport.compact.db /data/duckport/duckport.db
sudo duckport start all          # 确认正常后再删除 .before-compact
```

## 第三方插件

`aster-ingestor`、`binance-open-interest` 等第三方插件在作者按 [`ingestor-dev-guide.md` 附录](./ingestor-dev-guide.md#13-附录旧插件升级到无主键写法) 升级之前：

- `duckport migrate nopk` 会跳过它们的表（未登记 `write_mode`），这些表保持原样；
- retention 会跳过带主键的表（状态 `skipped_has_index`）。

## 全新部署

直接安装 1.1.0 和新插件，表从一开始就没有主键，不需要第 3、4 步。只需要近期数据时，把 ingestor 的 `START_DATE` 设为“今天 − 保留天数 − 7 天”，避免一开始灌入全量历史，然后按第 5 步开启 retention。
