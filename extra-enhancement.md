# Extra Enhancement — 并发模型

## D1. 并发模型（Q3 的细化）

Q3 是 "单表单写入线程"。这能有两种实现：

### D1a：Per-table Actor

每张用户表起一个独立 actor（`mpsc::Receiver<WriteRequest>`），接到请求时从池里借连接执行。meta 表 `ingestor_watermarks` 单独一个 actor。

- **优点**：不同表的写互不阻塞（K 线表和 funding 表可以并行写入）
- **缺点**：多个 actor 持有独立连接时 DuckDB MVCC 可能产生 `TransactionConflict`（需要重试逻辑）

### D1b：Single Writer（当前方案）

按 DuckDB 连接分片——全局只有 1 个 writer 连接 + 1 个 writer 线程，所有写请求 FIFO 串行化。读则走单独的读连接池（多条）。

- **优点**：最简单最可靠，无冲突
- **缺点**：吞吐下限就是单线程顺序写

### 决策

duckport 的实际写入 QPS 很低（每个 K 线周期每个数据源一次 batch），**D1b 起步**，压测不够用再升 D1a。
