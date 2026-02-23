"""基于 Spark + Parquet 的 CDC ingestion 示例作业。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class CDCIngestionConfig:
    source_path: str
    target_path: str
    checkpoint_path: str
    primary_keys: list[str]
    sequence_col: str
    operation_col: str = "op"
    delete_flag: str = "D"


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def _validate_config(config: CDCIngestionConfig) -> None:
    if not config.primary_keys:
        raise ValueError("primary_keys 不能为空。")


def _validate_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    existing = set(df.columns)
    missing = [c for c in required_columns if c not in existing]
    if missing:
        raise ValueError(f"输入数据缺失必要列: {missing}")


def _read_last_sequence(spark: SparkSession, checkpoint_path: str, sequence_col: str) -> Any | None:
    if not _path_exists(spark, checkpoint_path):
        return None

    checkpoint_df = spark.read.parquet(checkpoint_path)
    if checkpoint_df.limit(1).count() == 0:
        return None

    _validate_columns(checkpoint_df, [sequence_col])
    row = checkpoint_df.select(F.max(F.col(sequence_col)).alias("max_seq")).first()
    return row["max_seq"] if row else None


def _latest_changes(cdc_df: DataFrame, primary_keys: Iterable[str], sequence_col: str) -> DataFrame:
    # 为避免同一主键同一 sequence 的不稳定排序，增加 operation_col 作为次序辅助可在上游预处理。
    window = Window.partitionBy(*[F.col(pk) for pk in primary_keys]).orderBy(F.col(sequence_col).desc())
    return cdc_df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def run_cdc_ingestion(spark: SparkSession, config: CDCIngestionConfig) -> None:
    _validate_config(config)

    source_df = spark.read.parquet(config.source_path)
    required_columns = [*config.primary_keys, config.operation_col, config.sequence_col]
    _validate_columns(source_df, required_columns)

    last_seq = _read_last_sequence(spark, config.checkpoint_path, config.sequence_col)
    if last_seq is not None:
        source_df = source_df.where(F.col(config.sequence_col) > F.lit(last_seq))

    if source_df.limit(1).count() == 0:
        print("[CDC] No new records to process.")
        return

    latest_df = _latest_changes(source_df, config.primary_keys, config.sequence_col)

    non_meta_cols = [c for c in latest_df.columns if c not in {config.operation_col, config.sequence_col}]

    op_col = F.upper(F.col(config.operation_col))
    delete_flag = config.delete_flag.upper()

    upsert_df = latest_df.where(op_col != F.lit(delete_flag)).select(*non_meta_cols)
    delete_keys_df = latest_df.where(op_col == F.lit(delete_flag)).select(*config.primary_keys)

    if _path_exists(spark, config.target_path):
        current_df = spark.read.parquet(config.target_path)
    else:
        current_df = spark.createDataFrame([], upsert_df.schema)

    # 1) 删除 CDC 标记为 delete 的主键
    kept_df = current_df.join(delete_keys_df, on=config.primary_keys, how="left_anti")

    # 2) upsert: 先删除旧值，再并入最新值
    upsert_keys_df = upsert_df.select(*config.primary_keys).dropDuplicates()
    kept_without_upsert_df = kept_df.join(upsert_keys_df, on=config.primary_keys, how="left_anti")
    merged_df = kept_without_upsert_df.unionByName(upsert_df)

    merged_df.write.mode("overwrite").parquet(config.target_path)

    latest_seq = source_df.agg(F.max(F.col(config.sequence_col)).alias("max_seq")).first()["max_seq"]
    spark.createDataFrame([{config.sequence_col: latest_seq}]).write.mode("overwrite").parquet(config.checkpoint_path)

    print(f"[CDC] Ingestion completed. latest_sequence={latest_seq}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Parquet CDC ingestion")
    parser.add_argument("--source-path", required=True, help="增量 CDC Parquet 输入路径")
    parser.add_argument("--target-path", required=True, help="目标快照 Parquet 路径")
    parser.add_argument("--checkpoint-path", required=True, help="checkpoint Parquet 路径")
    parser.add_argument("--primary-keys", required=True, help="逗号分隔主键列，例如 id 或 id,tenant_id")
    parser.add_argument("--sequence-col", required=True, help="用于排序的递增序列列，例如 cdc_ts 或 cdc_seq")
    parser.add_argument("--operation-col", default="op", help="CDC 操作列（默认 op）")
    parser.add_argument("--delete-flag", default="D", help="删除操作标识（默认 D）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("parquet-cdc-ingestion").getOrCreate()

    config = CDCIngestionConfig(
        source_path=args.source_path,
        target_path=args.target_path,
        checkpoint_path=args.checkpoint_path,
        primary_keys=[pk.strip() for pk in args.primary_keys.split(",") if pk.strip()],
        sequence_col=args.sequence_col,
        operation_col=args.operation_col,
        delete_flag=args.delete_flag,
    )

    run_cdc_ingestion(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
