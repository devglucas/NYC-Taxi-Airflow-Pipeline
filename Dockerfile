FROM apache/airflow:3.4.0-python3.14
RUN pip install --no-cache-dir duckdb==2.0.0 matplotlib==3.11.2 pytest==9.1.1
