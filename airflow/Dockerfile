FROM apache/airflow:slim-3.0.1-python3.10

LABEL maintainer="Alisahib Gadimov"

USER root

# Copy initialization script
COPY init-airflow.sh init-airflow.sh
RUN chmod +x init-airflow.sh

# Install basic system utilities (removed Java and Spark-related tools)
RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends \
        bash \
        curl \
        wget \
        vim-tiny \
        nano \
        iputils-ping \
        telnet \
        openssh-client \
        net-tools \
        unzip \
        bc \
        sudo \
        build-essential && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python requirements
COPY requirements.txt requirements.txt
RUN pip install apache-airflow==3.0.1 -r requirements.txt