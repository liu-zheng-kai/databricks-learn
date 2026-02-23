# Spark + Parquet CDC Ingestion 示例

这个仓库提供了一个基于 **PySpark + Parquet** 的 CDC ingestion 实现，目标是把增量变更流（I/U/D）持续合并为一份最新快照。

## 功能说明

- 读取 CDC 增量 Parquet 数据。
- 通过 checkpoint 记录上次处理到的序列，支持增量消费。
- 对同一主键只保留本批次最新变更（按 `sequence_col`）。
- 支持删除（`D`）和 upsert（`I`/`U`）。
- 操作类型匹配大小写不敏感（例如 `d` 与 `D` 等价）。
- 启动作业时会校验关键列是否存在，提前失败并给出错误信息。
- 输出最新快照到目标 Parquet。

## CDC 输入格式要求

CDC 输入需要至少包含：

- 主键列（一个或多个）
- 业务数据列
- 操作列（默认 `op`，删除标识默认 `D`）
- 顺序列（`sequence_col`，可用时间戳或递增序列）

例如：

| id | name  | amount | op | cdc_seq |
|----|-------|--------|----|---------|
| 1  | Alice | 100    | I  | 1       |
| 1  | Alice | 120    | U  | 2       |
| 2  | Bob   | 50     | D  | 3       |

## 运行方式

```bash
spark-submit cdc_ingestion.py \
  --source-path /data/cdc_input \
  --target-path /data/snapshot \
  --checkpoint-path /data/checkpoint/cdc_job_1 \
  --primary-keys id \
  --sequence-col cdc_seq \
  --operation-col op \
  --delete-flag D
```

多主键示例：

```bash
--primary-keys id,tenant_id
```

## 核心处理逻辑

1. 读取 checkpoint，过滤掉已消费序列。
2. 对本批次 CDC 按主键取最新一条。
3. 分离 delete keys 和 upsert rows。
4. 从当前快照移除 delete keys。
5. 从快照移除 upsert keys 的旧数据，并合并 upsert rows。
6. 覆盖写入目标快照，并更新 checkpoint。

## 注意事项

- 该实现是“快照重写”模式（每批覆盖目标快照），适用于中小规模或演示场景。
- 超大规模场景建议使用 Delta Lake/Hudi/Iceberg 等支持原生 merge/upsert 的表格式以获得更好性能和并发能力。
- 对于同主键+同序列值并发事件，建议上游增加更细粒度排序字段并在入湖前做去重，以避免结果不确定性。
