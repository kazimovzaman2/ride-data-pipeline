FROM python:3.10-slim-bullseye

# Install OpenJDK 17 and essential tools
RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    libzbar-dev \
    bash \
    gcc \
    git \
    libc-dev \
    curl \
    wget \
    vim-tiny \
    nano \
    iputils-ping \
    telnet \
    openssh-client \
    openssh-server \
    net-tools \
    man \
    unzip \
    bc \
    thrift-compiler \
    sudo \
    build-essential && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Install Spark 3.5.0
RUN curl -L -o spark-3.5.0-bin-hadoop3.tgz https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Add Hadoop-AWS, AWS SDK, and PostgreSQL JDBC driver
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /opt/spark/jars/postgresql-42.3.5.jar https://jdbc.postgresql.org/download/postgresql-42.3.5.jar

# Create Spark history folder
RUN mkdir -p /opt/spark/history

# Copy Spark config and scripts
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD spark-master.sh spark-worker.sh /
COPY delta-core_2.12-2.4.0.jar /opt/spark/jars
