FROM apache/airflow:2.9.1

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

RUN --mount=type=bind,target=./requirements.txt,src=./requirements.txt \
    pip install -r requirements.txt

COPY --chown=50000 ./dags /opt/airflow/dags

WORKDIR /app