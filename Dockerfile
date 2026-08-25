FROM apache/airflow:2.9.3-python3.11

USER root

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /requirements.txt

RUN python -c "import minio; print('minio OK:', minio.__file__)"