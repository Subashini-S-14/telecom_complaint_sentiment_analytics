FROM python:3.11-slim

# -------- INSTALL SYSTEM DEPENDENCIES --------
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# -------- JAVA ENV --------
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# -------- INSTALL PYTHON + PYSPARK --------
RUN pip install --no-cache-dir pyspark

# -------- DOWNLOAD KAFKA-SPARK CONNECTOR (KEEPED ✅) --------
RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -O /opt/spark/jars/spark-sql-kafka.jar

# -------- HADOOP ENV (BASIC SETUP) --------
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

# -------- INSTALL PROJECT DEPENDENCIES --------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------- WORKING DIRECTORY --------
WORKDIR /workspace

# -------- DEFAULT COMMAND --------
CMD ["tail", "-f", "/dev/null"]