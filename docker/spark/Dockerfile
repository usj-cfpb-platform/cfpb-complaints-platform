# Spark image: Python 3.11 + Java 17 + PySpark 3.5.3 (local mode) + S3A jars for MinIO.
FROM python:3.11-slim-bookworm

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    SPARK_EXTRA_JARS_DIR=/opt/spark-jars

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# S3A connector so Spark can read/write MinIO (s3a://). Retries help on unstable networks.
RUN mkdir -p ${SPARK_EXTRA_JARS_DIR} \
    && curl -fL --retry 10 --retry-all-errors --retry-delay 5 \
       -o ${SPARK_EXTRA_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fL --retry 10 --retry-all-errors --retry-delay 5 \
       -o ${SPARK_EXTRA_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

WORKDIR /app
COPY requirements.txt requirements-spark.txt ./
   RUN --mount=type=cache,target=/root/.cache/pip \
       env -u PIP_NO_CACHE_DIR pip install --retries 10 --timeout 300 -r requirements-spark.txt

# Spark UI
EXPOSE 4040
