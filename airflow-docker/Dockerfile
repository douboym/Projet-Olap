# Use the official Apache Airflow image
FROM apache/airflow:2.5.1-python3.8

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH="$SPARK_HOME/bin:$PATH"

# Install dependencies for Spark and PostgreSQL
USER root
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk curl && \
    apt-get clean

COPY ./ressources/postgresql-42.7.4.jar /opt/spark/jars/

# Download and install Spark
RUN curl -fSL "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# Install Python dependencies for Spark and PostgreSQL
USER airflow
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    apache-airflow-providers-apache-spark==2.0.1 \
    psycopg2-binary

# Adjust permissions for Airflow user
USER root
RUN chown -R airflow: ${SPARK_HOME}
USER airflow