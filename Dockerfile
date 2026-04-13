FROM apache/airflow:2.8.0

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc g++ libkrb5-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
  requests \
  numpy \
  psycopg2-binary \
  apache-airflow-providers-postgres
