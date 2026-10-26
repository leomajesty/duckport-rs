# 升级 duckport

从任何旧版本升级都用同一条命令。它不按版本一步步迁移，而是检查服务端、插件和库的当前状态，把每一处与当前版本期望状态不一致的地方修正过来；已经一致的部分直接跳过，所以可以重复执行，中途失败后再执行一次即可继续。

```bash
sudo duckport upgrade --plan --proxy http://localhost:1082   # 只看计划，不修改（会 git fetch）
sudo duckport upgrade --proxy http://localhost:1082          # 执行
```

`--proxy` 用于 git、下载 release 和安装插件，本机的 gRPC 连接不走代理；不需要代理时省略。`--to v1.2.0` 升到指定版本（默认是仓库 upstream 的最新版本）。

## 三个阶段

1. **服务端**：`git pull`（或 checkout `--to` 指定的 tag），然后用拉下来的新 CLI 继续；`deploy.sh` 取 `v<Cargo.toml 中的版本>` 的 release binary（没有对应 release 时在本机编译）。binary 或运行中的版本变了才重启服务端，重启期间停止 ingestor。
2. **插件**：每个实例从 registry 记录的来源重新安装（同级目录有插件源码时用本地源码），版本变了就重启；第 1 步停掉的实例在这里启动。
3. **数据**：和 `duckport doctor` 做同样的检查，逐项修正（规则见 [升级对齐方案](http://192.168.9.20:8088/duckport-upgrade-reconcile-plan.html)）。只有在线修正时 ingestor 不停；需要停写时，所有修正在一个窗口里完成：停止 ingestor（服务端不停）→ 备份 → 修正 → 启动 ingestor → 复检。

只做第 3 步：`sudo duckport upgrade --data`。

## 需要人确认的地方

- **备份**：停写窗口一开始，服务端用 `COPY FROM DATABASE` 把整个库复制到 `<库目录>/backup/duckport-<run_id>.db`，逐表核对行数后才开始修改；恢复时停服并用它替换库文件（删除 `.wal`）。计划阶段估算需要的磁盘（已用块 + 重建空间）和内存（带主键的表在副本里重建索引，每行约 100 字节），与 `memory_limit`、本机可用内存比较。不够时给出警告，必须输入 `go on without backup` 才继续（非交互执行用 `--no-backup`）。不备份也能回退：重建的表保留为 `<表>__pk_old`，删除的行先导出到 `<库目录>/quarantine/<run_id>/*.parquet`。磁盘连重建都不够时直接阻断。
- **损坏或重复的行**：计划里列出行数和样例；`--data` 单独询问是否删除，确认后先导出到隔离文件再删除，重复的每组保留最后写入的一行。不确认就保留这些行，其他修正照常执行。`--yes` 不包括这一项，非交互执行要删除时加 `--delete-bad-rows`。
- **阻断**：写入方插件版本过低、上次重建的 `<表>__pk_old` 还在、磁盘不够重建时，不做任何修改，只给出处理办法。

## 收尾与回退

```bash
sudo duckport upgrade --finalize   # 观察无误后删除 <表>__pk_old（不需要停写）
sudo duckport upgrade --rollback   # 把 <表>__pk_old 恢复为原表，重建后新写入的数据会补回（停写）
```

每一步记录在 `data.upgrade_log`（`run_id` 与备份、隔离目录同名）。`duckport doctor` 随时可以只读地检查当前状态。

## 从 1.1.0 及更早版本第一次升级

旧版本的 CLI 里，`duckport upgrade` 只做 `git pull` 和 `deploy.sh`；执行一次后新 CLI 已安装，再执行一次 `sudo duckport upgrade` 就会走上面的完整流程。

## 插件的表规格

插件在 `data.table_specs` 登记自己写的每张表的规格（列、去重键、时间列和周期、字符串校验、`write_mode`），doctor / upgrade 按规格检查（见 [`ingestor-dev-guide.md` 5.3](./ingestor-dev-guide.md#53-登记表规格)）。没有登记的表（例如第三方插件）只做通用检查，带主键时只提示、不会自动去主键。duckport-rs 只内置自己维护的插件（binance-ingestor、binance-funding）各历史版本的规格。

## 同一台机器上的第二套部署

演练或测试时可以在同一台机器上装第二套，互不影响：

```bash
sudo DEPLOY_OPT=/srv/dp2/opt DEPLOY_DATA=/srv/dp2/data DUCKPORT_UNIT_PREFIX=dp2 DUCKPORT_CLI_DIR=/srv/dp2/bin \
  LISTEN_ADDR=127.0.0.1:50081 bash deploy.sh
sudo DUCKPORT_HOME=/srv/dp2/opt /srv/dp2/bin/duckport status
```

systemd unit 名为 `dp2-server`、`dp2-ingestor@<实例>`；unit 前缀和 CLI 目录记在 `$DEPLOY_OPT` 下，之后的升级沿用。实例的 `config.env` 里写上 `DUCKPORT_LISTEN_ADDR`，避免插件读到默认部署的地址。

## 怎样验证的

- **样本库**（`tests/fixtures/build_fixtures.py`，CI 中每次运行）：用各历史版本的服务端 release binary 和插件对应提交的代码生成 F1–F6——v1.0.1 + 插件首版、v1.0.4 + 插件 0.1.0（exginfo 膨胀、损坏行、staging 残留）、服务端新插件旧、表已去主键插件旧、当前目标状态、带主键的未知表。`tests/python/test_fixtures.py` 对每个样本库：升级后 doctor 零差异、第二次什么都不做，并逐表核对“升级后行数 + 隔离文件行数 = 升级前行数”，行哈希之和同样相等。
- **沙箱端到端**（`tests/e2e/upgrade_sandbox.sh`）：在同一台机器上装第二套（v1.0.4 + 插件 0.1.0，真实采集 Binance 数据），执行 `duckport upgrade`，检查服务端重启到新版本、插件重装并在运行、数据对齐、ingestor 升级后继续写入、第二次执行什么都不做。
- **大规模**：在 pro.studio 升级前备份（186 GB）的副本上执行，结果见下。

2026-10-26 在 pro.studio 升级前备份的副本上实测（服务端用当前代码，内存上限 16 GB）：

| 项目 | 结果 |
|---|---|
| 起点 | v1.0.4 + 插件 0.1.0；186 GB，带主键的业务表 19.6 亿行；exginfo 物理 2.13 亿行、有效 1,260 行，其中 1 行损坏 |
| 检查 | 旧插件下 doctor 17 秒（阻断：插件版本、带主键的表）；升级后插件下的计划 3 秒 |
| 计划 | 预计停写 141 秒；备份不可行：带主键的表在副本里重建索引约需 197 GB 内存，可用 16 GB，需要确认 `go on without backup` |
| 停写窗口 | 130 秒：4 张表重建（`usdt_perp_1m` 8.84 亿行 55 秒，`usdt_spot_1m` 10.7 亿行 71 秒），损坏行隔离后删除，删除 `retention_tasks` |
| 复检 | doctor 6 秒，零差异；再执行一次什么都不做；finalize 2 秒 |
| 数据 | 各表行数不变（exginfo 1,260 = 1,259 + 隔离的 1 行） |
| 磁盘 | 库文件 186 GB → 273 GB；finalize 后已用 99 GiB，其余块留给后续写入复用，或停服压缩 |
