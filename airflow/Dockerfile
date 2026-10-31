FROM apache/airflow:2.10.2
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 "pyarrow<19" "numpy==1.26.4" pandas deltalake
