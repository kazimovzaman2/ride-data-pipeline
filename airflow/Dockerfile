FROM apache/airflow:slim-3.0.1-python3.10

LABEL maintainer="Zaman Kazimov"

USER root

COPY init-airflow.sh init-airflow.sh
RUN chmod +x init-airflow.sh

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

COPY requirements.txt requirements.txt
RUN pip install apache-airflow==3.0.1 -r requirements.txt
