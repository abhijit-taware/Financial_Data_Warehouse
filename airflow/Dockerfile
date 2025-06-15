# ./airflow/Dockerfile

FROM apache/airflow:2.9.1-python3.10

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark manually (matching the Spark version in your Spark cluster)
ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Switch back to non-root airflow user
USER airflow

COPY requirements.txt /requirements.txt

ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt"

RUN pip install --no-cache-dir -r /requirements.txt --constraint "${AIRFLOW_CONSTRAINTS_URL}"
