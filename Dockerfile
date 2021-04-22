FROM puckel/docker-airflow

VOLUME /Users/alexei/docs/MIAGE/S4/D605/darties/airflow /usr/local/airflow

WORKDIR /airflow
COPY requirements.txt /airflow
RUN pip install -U pip && pip install -r requirements.txt

EXPOSE 8080

