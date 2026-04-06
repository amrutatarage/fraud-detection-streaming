FROM apache/flink:1.20.0-scala_2.12-java17

USER root

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.20.0 scikit-learn joblib "numpy<2" psycopg2-binary faker requests

# Add exact Kafka connector for Flink 1.20.0
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

# Copy jobs
COPY flink-jobs /opt/flink/jobs