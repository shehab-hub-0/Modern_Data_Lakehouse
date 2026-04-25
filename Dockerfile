FROM apache/airflow:3.1.2
ADD requirements.txt requirements.txt 
RUN pip install -r requirements.txt