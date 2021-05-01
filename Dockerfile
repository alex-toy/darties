FROM puckel/docker-airflow

WORKDIR /usr/local/airflow

COPY requirements.txt /usr/local/airflow

RUN pip install -U pip && pip install -r requirements.txt

EXPOSE 8080

