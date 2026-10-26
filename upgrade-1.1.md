# 升级到 duckport-rs 1.1.0

1.1.0 带来三项变化：

- **DuckDB 1.5.1 → 1.5.6**。修复 [duckdb#22279](https://github.com/duckdb/duckdb/issues/22279)：带主键的表在主键冲突导致写入回滚后，后续写入的字符串会混入被回滚的内容并落盘。存储格式不变，不需要转换库文件。
- **新的 retention**。`DUCKPORT_RETENTION_ENABLED` 改为按天滚动删除，规则在 `data.retention_policy`，默认关闭，打开后默认先 dry-run。旧的按月 `COPY+DELETE` 逻辑和 `DUCKPORT_RETENTION_TABLE` 废弃，`data.retention_tasks` 不再使用。
- **业务表不再建主键**。DuckDB 1.5 不回收带索引表删除的行，retention 删了也控制不住磁盘；联合主键也不参与查询，写入反而更慢。插件改用 anti-join 去重写入（原因和实测见 [`ingestor-dev-guide.md` 5.1](./ingestor-dev-guide.md#51-为什么业务表不建主键)）。

只升级服务端、不做后面几步也可以正常运行，行为与 1.0.x 相同。要用 retention 或修复 `exginfo` 膨胀，需要完成全部步骤。

## 升级顺序

必须先升级插件、再对齐数据（第 3 步会检查，旧插件还在写的表会阻断）：旧插件的 `ON CONFLICT DO NOTHING` 遇到无主键的表会直接报错，而新插件在带主键的旧表上同样能正确写入。

| | 表带主键（迁移前） | 表无主键（迁移后） |
|---|---|---|
| 旧插件（`ON CONFLICT DO NOTHING`） | 正常 | 报错 `There are no UNIQUE/PRIMARY KEY constraints that refer to this table` |
| 新插件（anti-join） | 正常 | 正常 |

### 1. 升级服务端（不停写）

```bash
cd <repo_dir>
git remote set-url origin https://github.com/sjmc-quant/duckport-rs.git   # 从 leomajesty clone 的部署目录需要改一次
git pull --ff-only
sudo env GITHUB_REPO=sjmc-quant/duckport-rs RELEASE_TAG=v1.1.0 bash ./deploy.sh   # 保留现有 server.env
sudo duckport restart server
duckport status            # 版本显示 v1.1.0，并多出 Retention 一节
```

仓库已统一到 `sjmc-quant`：`deploy.sh` 默认从 `sjmc-quant/duckport-rs` 下载 release。服务端启动时会自动创建 `data.retention_policy` 和 `data.retention_log`。`DUCKPORT_RETENTION_ENABLED` / `DUCKPORT_RETENTION_DRY_RUN` 现在只接受 `true`/`false`（或 `1`/`0`、`yes`/`no`、`on`/`off`），空值或拼错会让服务端拒绝启动，升级前先检查 server.env。通过 Airport 连接的 DuckDB 客户端，`duckdb` 版本同步升到 1.5.6。

回退：重新部署 v1.0.4。

### 2. 升级插件（逐个实例重启）

```bash
sudo duckport update <实例名>     # 每个实例执行一次
duckport status                   # 确认 watermark 继续推进
```

需要的最低版本：binance-ingestor 0.2.0、binance-funding 0.2.0。新插件启动时会在 `data.config_dict` 登记 `write_mode:<表名> = anti_join`，第 3 步据此判断哪些表可以迁移。

回退：装回旧版本（第 3 步之前）。

### 3. 对齐数据（停写约 1 分钟 / 10 亿行，加上备份时间）

先看计划，不修改任何东西：

```bash
sudo duckport upgrade --plan
```

它和 `duckport doctor` 做同样的检查（规则见 [升级对齐方案](http://192.168.9.20:8088/duckport-upgrade-reconcile-plan.html)），列出每项修正属于“在线”还是“停写”，以及停写窗口要做什么：带主键的业务表重建为无主键表、`exginfo` 去膨胀、删除废弃的 `data.retention_tasks` 和 `config_dict` 旧 key、列出损坏或重复的行（删除前人工确认）等。写这张表的插件还是旧版本时会阻断，先完成第 2 步。

然后执行：

```bash
sudo duckport upgrade --data
```

需要停写时，所有修正在一个窗口里完成：停止正在运行的 ingestor（服务端不停）→ 备份 → 修正 → 启动 ingestor → 复检（必须与期望状态一致）。

- **备份**：服务端用 `COPY FROM DATABASE` 把整个库复制到 `<库目录>/backup/duckport-<run_id>.db`，逐表核对行数后才开始修改。恢复：停服后用它替换 `duckport.db`（并删除 `duckport.db.wal`）。不带主键的表复制约 2 GB/s；带主键的表要在副本里重建索引，约 150 万行/秒，每行约需 100 字节内存。
- **磁盘或内存不够备份**：计划里会给出警告（例如 pro.studio 升级前 19.6 亿行带主键，重建索引约需 196 GB 内存，远超 `memory_limit`）。`--data` 会要求输入 `go on without backup` 才继续；也可以直接加 `--no-backup`。不备份也能回退：重建时原表保留为 `<表>__pk_old`，删除的行都先导出到 `<库目录>/quarantine/<run_id>/*.parquet`。磁盘连重建都不够时直接阻断。
- 重建：对每张表建一张同结构、无约束的新表，按月 `INSERT ... ORDER BY 时间列` 拷贝 → 核对行数 → 在一个事务里把原表改名为 `<表>__pk_old`、新表改为原名。
- `exginfo` 一并重建：只拷贝有效行，带主键时累积的已删除行随之消失。
- **损坏或重复的行要人工确认**：计划里列出行数和样例（损坏的字符串截短显示长度，重复的列出键和份数），`--data` 会单独询问是否删除；确认后先导出到隔离文件再删除，重复的每组保留最后写入的一行。不确认就保留这些行，其他修正照常执行。`--yes` 不包括这一项，非交互执行要删除时加 `--delete-bad-rows`。
- 每一步记入 `data.upgrade_log`（`run_id` 与备份、隔离目录同名）。中途失败可以直接再次执行，已对齐的部分会跳过。
- 耗时参考（2026-10-26 在 pro.studio 库文件副本上实测，19.6 亿行、186 GB）：重建共 130 秒（`usdt_perp_1m` 8.8 亿行 56 秒，`usdt_spot_1m` 10.7 亿行 72 秒，其余不到 2 秒）；期间库文件临时涨到 273 GB。回退 2 秒，finalize 2 秒。重建后 consumer 常用查询的耗时不变。
- 重建后数据按时间重新排序，数据部分比原来按 symbol/月导入的布局大 20%–28%（压缩率略低），但去掉了主键索引，整体占用从 71 万块降到 40.5 万块（约 99 GiB）。

**回退**（第 4 步之前随时可以）：

```bash
sudo duckport upgrade --rollback
```

把 `<表>__pk_old` 恢复为原表，重建后新写入的数据会按主键补回旧表（包括重建时为空的表，以及回填到旧时间段的数据）；`exginfo` 用当前快照替换。新插件在带主键的表上也能正常写入，回退后不需要降级插件。

### 4. 清理旧表（观察 24 小时后）

```bash
sudo duckport upgrade --finalize
```

删除所有 `<表>__pk_old` 并 CHECKPOINT（只删这些旧表，不需要停 ingestor）。释放的空间留给后续写入复用，库文件本身不会变小；主键索引占用的内存随之释放。要真正缩小文件见下面的“压缩库文件”（实测 186 GB → 100 GB）。确认无误后手动删除第 3 步的备份文件。

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

## 在副本上演练

正式执行第 3 步前，用库文件副本演练一次，拿到准确耗时。在第 1、2 步之后做：这时 `/opt/duckport/bin/duckport-server` 已是 1.1.0，插件也已在 `data.config_dict` 登记 `write_mode`，副本里自然带着这些登记。

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
duckport_upgrade() {                        # 副本上没有在跑的 ingestor，直接调用对齐脚本
  $PY <repo_dir>/scripts/upgrade.py --addr 127.0.0.1:50061 --db-path $PWD/duckport.db "$@"
}
duckport_upgrade plan
time duckport_upgrade apply --levels online,stop            # 演练要测备份耗时就加 --backup

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

- `duckport upgrade` 不会给它们的表去主键（未登记 `write_mode`，只给提示），这些表保持原样；
- retention 会跳过带主键的表（状态 `skipped_has_index`）。

## 全新部署

直接安装 1.1.0 和新插件，表从一开始就没有主键，不需要第 3、4 步。只需要近期数据时，把 ingestor 的 `START_DATE` 设为“今天 − 保留天数 − 7 天”，避免一开始灌入全量历史，然后按第 5 步开启 retention。
