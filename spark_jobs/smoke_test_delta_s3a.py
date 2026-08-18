"""
One-off proof that the PySpark + Delta Lake + S3A(MinIO) wiring in
common/spark_session.py actually works, before it's trusted under the
four real bronze jobs. Not part of the orchestrated DAG.

Writes a tiny Delta table to MinIO, reads it back, appends a second
batch, and confirms `_delta_log` shows two commits -- i.e. this is a
real transactional Delta table, not parquet-with-extra-steps.

Run inside the airflow-scheduler container (it has Java + PySpark +
delta-spark installed; the host venv deliberately does not):
    docker compose --profile orchestration exec airflow-scheduler \
        python /opt/airflow/spark_jobs/smoke_test_delta_s3a.py
"""
from common.spark_session import get_spark, lakehouse_path

TABLE_PATH = lakehouse_path("bronze", "_smoke_test")


def main() -> None:
    spark = get_spark("smoke-test-delta-s3a")
    spark.sparkContext.setLogLevel("WARN")

    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df1.write.format("delta").mode("overwrite").save(TABLE_PATH)
    print(f"wrote batch 1 to {TABLE_PATH}")

    df2 = spark.createDataFrame([(3, "c")], ["id", "val"])
    df2.write.format("delta").mode("append").save(TABLE_PATH)
    print("appended batch 2")

    result = spark.read.format("delta").load(TABLE_PATH)
    rows = sorted(r["id"] for r in result.collect())
    assert rows == [1, 2, 3], f"expected [1, 2, 3], got {rows}"
    print(f"read back {result.count()} rows: OK")

    history = spark.sql(f"DESCRIBE HISTORY delta.`{TABLE_PATH}`")
    history.select("version", "operation").show(truncate=False)
    n_versions = history.count()
    assert n_versions == 2, f"expected 2 Delta commits (overwrite + append), got {n_versions}"
    print(f"Delta transaction log shows {n_versions} commits: OK")

    print("\nSMOKE TEST PASSED: PySpark + Delta Lake + S3A(MinIO) wiring is sound.")
    spark.stop()


if __name__ == "__main__":
    main()
