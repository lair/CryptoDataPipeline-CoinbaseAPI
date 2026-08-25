FROM apache/airflow:2.9.3-python3.11

# Copia o arquivo de dependências extras do projeto
COPY requirements.txt /requirements.txt

# Instala as dependências respeitando as constraints oficiais do Airflow,
# evitando conflitos de versão com libs internas (ex: SQLAlchemy, Flask-AppBuilder)
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /requirements.txt