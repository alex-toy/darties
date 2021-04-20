FROM puckel/docker-airflow:1.10.9
WORKDIR /airflow
COPY requirements.txt /airflow
RUN pip install -U pip && pip install -r requirements.txt


