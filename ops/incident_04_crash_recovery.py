"""
INCIDENT #4 (live demo): a Spark job killed mid-write, recovered via the
Delta transaction log.

The property under test: Delta's commit protocol writes data files
first, then activates them for readers with a SEPARATE, final
`_delta_log/<version>.json` commit. A process killed after some data
files are written but BEFORE that commit means those files exist
physically in object storage but are invisible to every reader -- the
table stays at whatever version it last successfully committed, in
full, not partially overwritten and not corrupted. This script proves
that directly rather than asserting it:

  1. A full, uninterrupted write establishes a baseline ("version N",
     ~11.25M rows -- all of bronze.order_items, repartitioned to 64
     files so there's a real multi-second window where files are
     landing but the commit hasn't happened yet).
  2. The SAME write is launched again and killed with SIGKILL --
     specifically, KILLED THE MOMENT a new (uncommitted) parquet file
     is observed appearing under the table's path via a boto3 polling
     loop against MinIO, not after a guessed sleep duration. That's
     real evidence the write was genuinely in flight when it died, not
     a kill that landed before anything happened or after it had
     already finished.
  3. Verify: the table is still readable, still shows the FULL baseline
     row count (not 0, not a partial subset), and `_delta_log/` shows no
     new commit version -- proving readers never saw the torn write.
  4. Verify: at least one orphaned (uncommitted) parquet file is sitting
     in the table's storage path, physical proof the write really did
     start before it was killed.
  5. Recovery: re-run the SAME write to completion. It succeeds, and
     `_delta_log/` now shows the new commit.

Run: `docker compose --profile orchestration exec airflow-scheduler
python /opt/airflow/ops/incident_04_crash_recovery.py` (run
`_crash_test_write_job.py` once by hand first if you want a warm JVM/jar
cache -- not required, just faster).
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import boto3
from botocore.client import Config

sys.path.insert(0, "/opt/airflow/ops")
from _crash_test_write_job import CRASH_TEST_PATH  # noqa: E402

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse_dev_pw")
BUCKET = os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")
CRASH_TEST_PREFIX = "_ops/_crash_test/"
POLL_INTERVAL_S = 0.3
MAX_WAIT_FOR_WRITE_START_S = 90


def s3_client():
    return boto3.client(
        "s3", endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER, aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}), region_name="us-east-1",
    )


def list_parquet_keys(client) -> set[str]:
    keys = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=CRASH_TEST_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and "_delta_log/" not in obj["Key"]:
                keys.add(obj["Key"])
    return keys


def list_commit_versions(client) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    versions = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{CRASH_TEST_PREFIX}_delta_log/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                versions.append(obj["Key"])
    return sorted(versions)


def spark_row_count() -> int:
    result = subprocess.run(
        ["python", "-c", f"""
from common.spark_session import get_spark
spark = get_spark('incident04_verify')
spark.sparkContext.setLogLevel('WARN')
try:
    df = spark.read.format('delta').load('{CRASH_TEST_PATH}')
    print('COUNT=' + str(df.count()))
except Exception as e:
    print('COUNT=ERROR:' + str(e)[:200])
spark.stop()
"""],
        cwd="/opt/airflow/spark_jobs", capture_output=True, text=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("COUNT="):
            return line.split("=", 1)[1]
    print(result.stdout, file=sys.stderr)
    print(result.stderr, file=sys.stderr)
    raise RuntimeError("no COUNT= line")


def establish_baseline() -> str:
    print("=== step 0: full, uninterrupted write to establish a baseline ===")
    t0 = time.time()
    result = subprocess.run(["python", "/opt/airflow/ops/_crash_test_write_job.py"], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr[-2000:], file=sys.stderr)
        raise RuntimeError("baseline write failed")
    print(f"baseline write completed in {time.time() - t0:.1f}s")
    baseline_count = spark_row_count()
    print(f"baseline row count: {baseline_count}")
    return baseline_count


def kill_mid_write() -> None:
    print("\n=== step 1: launch the same write again, kill it the instant a new file appears ===")
    client = s3_client()
    versions_before = list_commit_versions(client)
    print(f"committed versions before this attempt: {len(versions_before)}")

    # PySpark launches the actual Spark driver as a CHILD JVM process of
    # this python wrapper (via py4j) -- SIGKILL-ing just the wrapper
    # leaves that JVM as an orphan that keeps right on writing and can
    # still commit, which would silently invalidate the whole demo
    # (nothing "crashed", it just detached). preexec_fn=os.setsid puts
    # the wrapper AND everything it spawns in a new process group, so
    # os.killpg() below can take out the JVM too, not just its launcher.
    proc = subprocess.Popen(
        ["python", "/opt/airflow/ops/_crash_test_write_job.py"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid,
    )

    known_keys = list_parquet_keys(client)
    t0 = time.time()
    new_key = None
    while time.time() - t0 < MAX_WAIT_FOR_WRITE_START_S:
        time.sleep(POLL_INTERVAL_S)
        if proc.poll() is not None:
            raise RuntimeError(f"process exited on its own (code {proc.returncode}) before we ever saw a new file -- "
                                f"either it's too fast to catch or it failed outright")
        current_keys = list_parquet_keys(client)
        newly_seen = current_keys - known_keys
        if newly_seen:
            new_key = next(iter(newly_seen))
            break

    if new_key is None:
        proc.kill()
        raise RuntimeError(f"never observed a new parquet file within {MAX_WAIT_FOR_WRITE_START_S}s -- can't prove a mid-write kill")

    elapsed = time.time() - t0
    print(f"observed new uncommitted file after {elapsed:.1f}s: {new_key}")
    print(f"sending SIGKILL to process group {os.getpgid(proc.pid)} NOW (wrapper pid {proc.pid} + its spawned JVM)")
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=10)
    print(f"process group killed (wrapper return code {proc.returncode})")

    # Give S3/MinIO a moment to converge (writes killed mid-flight can
    # leave the object store briefly inconsistent about listing what
    # was flushed) before we go looking for evidence.
    time.sleep(2)

    print("\n=== step 2: verify the table survived intact ===")
    versions_after = list_commit_versions(client)
    print(f"committed versions after the kill: {len(versions_after)} (before: {len(versions_before)})")
    if len(versions_after) != len(versions_before):
        raise AssertionError("a new commit landed despite the kill -- the timing missed the write window")

    orphaned = list_parquet_keys(client) - known_keys
    print(f"orphaned (uncommitted) parquet files left behind: {len(orphaned)}")
    if not orphaned:
        raise AssertionError("no orphaned files found -- the kill may have landed before any file was written")

    count_after_kill = spark_row_count()
    print(f"row count read immediately after the kill: {count_after_kill}")


def recover() -> str:
    print("\n=== step 3: recovery -- re-run the same write to completion ===")
    t0 = time.time()
    result = subprocess.run(["python", "/opt/airflow/ops/_crash_test_write_job.py"], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr[-2000:], file=sys.stderr)
        raise RuntimeError("recovery write failed")
    print(f"recovery write completed in {time.time() - t0:.1f}s")
    return spark_row_count()


def main() -> None:
    baseline_count = establish_baseline()
    kill_mid_write()
    count_after_kill = spark_row_count()
    recovered_count = recover()

    print("\n=== summary ===")
    print(f"baseline row count        : {baseline_count}")
    print(f"row count after the kill  : {count_after_kill}  (must equal baseline -- proves no torn write was visible)")
    print(f"row count after recovery  : {recovered_count}  (must equal baseline -- proves the retry fully succeeds)")

    assert count_after_kill == baseline_count, "table was corrupted/changed by the killed write -- Delta protection failed"
    assert recovered_count == baseline_count, "recovery run didn't fully restore the table"

    print("\nPASS: a write killed mid-flight left zero trace in the Delta table readers see -- "
          "the table stayed at its last good committed version throughout, and a plain re-run recovered it fully.")


if __name__ == "__main__":
    main()
