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

服务端启动时会自动创建 `data.retention_policy` 和 `data.retention_log`。`DUCKPORT_RETENTION_ENABLED` / `DUCKPORT_RETENTION_DRY_RUN` 现在只接受 `true`/`false`（或 `1`/`0`、`yes`/`no`、`on`/`off`），空值或拼错会让服务端拒绝启动，升级前先检查 server.env。通过 Airport 连接的 DuckDB 客户端，`duckdb` 版本同步升到 1.5.6。

回退：重新部署 v1.0.4。

### 2. 升级插件（逐个实例重启）

```bash
sudo duckport update <实例名>     # 每个实例执行一次
duckport status                   # 确认 watermark 继续推进
```

需要的最低版本：binance-ingestor 0.2.0、binance-funding 0.2.0。新插件启动时会在 `data.config_dict` 登记 `write_mode:<表名> = anti_join`，第 3 步据此判断哪些表可以迁移。

回退：装回旧版本（第 3 步之前）。

### 3. 迁移表结构（停写约 1 分钟 / 10 亿行）

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
- 只想迁移部分表：`sudo duckport migrate nopk --tables usdt_perp_1m usdt_spot_1m`（`--plan --tables ...` 先看计划）。
- 耗时参考（2026-10-26 在 pro.studio 库文件副本上实测，19.6 亿行、186 GB）：迁移共 130 秒（`usdt_perp_1m` 8.8 亿行 56 秒，`usdt_spot_1m` 10.7 亿行 72 秒，其余不到 2 秒）；迁移期间库文件临时涨到 273 GB。回退 2 秒，finalize 2 秒。迁移后 consumer 常用查询的耗时不变。
- 迁移后数据按时间重新排序，数据部分比原来按 symbol/月导入的布局大 20%–28%（压缩率略低），但去掉了主键索引，整体占用从 71 万块降到 40.5 万块（约 99 GiB）。

**回退**（第 4 步之前随时可以）：

```bash
sudo duckport migrate nopk --rollback
```

把 `<表>__pk_old` 恢复为原表，迁移后新写入的数据会按主键补回旧表（包括迁移时为空的表，以及回填到旧时间段的数据）；`exginfo` 用当前快照替换。新插件在带主键的表上也能正常写入，回退后不需要降级插件。

### 4. 清理旧表（观察 24 小时后）

```bash
sudo duckport migrate nopk --finalize
```

删除所有 `<表>__pk_old` 并 CHECKPOINT（只删这些旧表，不需要停 ingestor）。释放的空间留给后续写入复用，库文件本身不会变小；主键索引占用的内存随之释放。要真正缩小文件见下面的“压缩库文件”（实测 186 GB → 100 GB）。

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

保留天数至少要覆盖策略的最长回看窗口再加余量（consumer 的 `get_market` / `get_symbol` 不传 `begin` 时默认回看 90 天）。

第一次真删会处理全部积压历史。pro.studio 副本实测（保留 120 天）：dry-run 3.7 秒；真删 17.5 亿行、约 2000 天用时 17 分钟（其中约 13 分钟是批次间让出写连接的停顿），删除期间每 2 秒一次的实时写入 p50 17 ms、p99 383 ms、最长 841 ms；删完剩余 2.04 亿行只占约 10 GiB，其余块留作复用。各状态的含义见 [`deploy.md` 4.3](./deploy.md#43-数据保留retention)。

## 在副本上演练迁移

正式迁移前，用库文件副本演练一次，拿到准确耗时。在第 1、2 步之后做：这时 `/opt/duckport/bin/duckport-server` 已是 1.1.0，插件也已在 `data.config_dict` 登记 `write_mode`，副本里自然带着这些登记。

复制副本需要短暂停服，保证文件一致：停服时长约为“库文件大小 ÷ 磁盘复制速度”再加 1 分钟左右（服务端打出 shutdown complete 后，进程还要一段时间才退出并释放文件锁，`duckport stop` 会等它退出）。pro.studio 上 186 GB、1.7 GB/s，共停写约 3 分钟；ingestor 重启后自动补拉缺口。ext4 不支持 `cp --reflink`，只能完整复制。

```bash
sudo -i                                     # 以下在 root shell 中执行
duckport stop all                           # 等到命令返回，进程已退出
mkdir -p /data/duckport-rehearsal
cp /data/duckport/duckport.db* /data/duckport-rehearsal/
duckport start all

# 在另一个端口上用副本起一个临时服务端（限制内存，避免和线上争抢）
cd /data/duckport-rehearsal
DUCKPORT_DB_PATH=$PWD/duckport.db DUCKPORT_LISTEN_ADDR=127.0.0.1:50061 \
  DUCKPORT_DUCKDB_MEMORY_LIMIT=16GB /opt/duckport/bin/duckport-server > server.log 2>&1 &
REHEARSAL_PID=$!                            # 记下它自己的 PID，不要用 pkill：命令行和线上进程完全一样
PY=/opt/duckport/ingestors/<任一实例>/.venv/bin/python
$PY <repo_dir>/scripts/migrate_nopk.py --addr 127.0.0.1:50061 --db-path $PWD/duckport.db plan
time $PY <repo_dir>/scripts/migrate_nopk.py --addr 127.0.0.1:50061 --db-path $PWD/duckport.db migrate

# 收尾：停掉临时服务端，等进程真正退出（释放文件锁）后再删除副本
kill -TERM $REHEARSAL_PID
while kill -0 $REHEARSAL_PID 2>/dev/null; do sleep 1; done
cd / && rm -rf /data/duckport-rehearsal
```

## 可选：压缩库文件

删除旧表后，释放的块只在文件内部复用，文件不会变小。需要真正释放磁盘时，停服后复制出一个紧凑的新文件。pro.studio 副本实测：71 秒，186 GB → 100 GB（finalize 之后），行数逐表核对一致；业务表不带主键，系统表的主键保留。

```bash
sudo duckport stop all
$PY - <<'EOF'
import duckdb   # 1.5.6
con = duckdb.connect(config={"memory_limit": "16GB"})
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
