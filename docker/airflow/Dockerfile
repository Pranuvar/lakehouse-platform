# Custom Airflow image bundling Java + PySpark + delta-spark + dbt-duckdb
# in one container. Why one image instead of a separate Spark cluster
# container: at this data volume (single-digit-GB Delta tables), a
# dedicated Spark master/worker pair is pure overhead -- local[*] mode
# gives every job all the cores/RAM the container has, and
# SparkSubmitOperator / BashOperator can invoke it directly. The only
# thing that changes to point this at a real cluster (EMR/Databricks/
# YARN) later is the --master value passed to spark-submit; none of the
# job code changes.
FROM apache/airflow:2.10.5-python3.11

USER root

# Java 17 -- required by Spark 3.5.x, not provided by the base image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# PySpark's own s3a support needs the matching hadoop-aws + aws-java-sdk-bundle
# jars, which aren't on PyPI -- pyspark's `--packages` resolves them via Maven
# at spark-submit time (cached after first run). Documented here rather than
# baked into the image so the exact versions stay next to the code that
# depends on them: see spark_jobs/common/spark_session.py.
