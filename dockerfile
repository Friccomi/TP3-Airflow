from apache/airflow:2.1.3
RUN pip install python-decouple SQLAlchemy  psycopg2-binary pandas python-decouple apache-airflow-providers-docker  apache-airflow-providers-ftp
RUN pip install apache-airflow-providers-http apache-airflow-providers-imap apache-airflow-providers-postgres apache-airflow-providers-sqlite
RUN pip install matplotlib