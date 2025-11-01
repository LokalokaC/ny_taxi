FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /opt/airflow
ENV PYTHONPATH="/opt/airflow:/opt/airflow/"
COPY requirements.txt .

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir -r requirements.txt --constraint "${CONSTRAINTS_URL}"

COPY dags/ ./dags
COPY src/ ./src
COPY plugins/ ./plugins
COPY app.ini ./app.ini
COPY .env.example ./.env.example