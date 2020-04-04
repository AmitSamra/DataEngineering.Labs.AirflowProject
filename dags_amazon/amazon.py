import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import csv
import requests
import os
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
	'owner':'Mr. Amit',
	'start_date': datetime.now(),
	'retries':0,
	'retry_delay':timedelta(minutes=1)
}


dag = DAG(
	'amazon',
	default_args = default_args,
	description = 'Amazon',
	#schedule_interval=timedelta(hours=1)
	)


def get_amazon_purchases():
	"""
	Gets my Amazon order history for 12 years
	"""
	url = 'https://airflowfiles.s3.amazonaws.com/amazon_purchases.csv'
	response = requests.get(url)
	with open('amazon_purchases.csv', 'wb') as f:
		f.write(response.content)


t1 = PythonOperator(
	task_id = 'get_amazon_purchases',
	python_callable = get_amazon_purchases,
	provide_context = False,
	dag = dag,
	)


task = PostgresOperator(
task_id='import_to_postgres',
postgres_conn_id='postgres_amazon',
sql="COPY amazon.amazon_purchases FROM '/Users/asamra/dev/DataEngineering.Labs.AirflowProject/raw_data/amazon_purchases.csv' DELIMITER ',' CSV HEADER;",
dag=dag,
)
