FROM puckel/docker-airflow

VOLUME ${PWD}/airflow /usr/local/airflow

WORKDIR /usr/local/airflow

COPY requirements.txt .

RUN pip install -U pip && pip install -r requirements.txt

EXPOSE 8080

