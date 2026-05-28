FROM apache/airflow:2.9.1
RUN pip install --no-cache-dir \
    apache-airflow-providers-sftp==4.9.0 \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-mongo==4.2.0