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
	description = 'amazon',
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


t2 = PostgresOperator(
	task_id = 'create_table',
	postgres_conn_id = 'postgres_amazon',
	sql = '''CREATE TABLE amazon.amazon_purchases(
		order_id integer primary key, 
		order_date date,
		category varchar(255),
		website varchar(255),
		condition varchar(255),	
		seller varchar(255),
		list_price_per_unit numeric,
		purchase_price_per_unit numeric,
		quantity integer,
		shipment_date date,	
		carrier_name varchar(255),
		item_subtotal numeric,
		item_subtotal_tax numeric,
		item_total numeric);''',
	dag = dag,
	)


t3 = PostgresOperator(
	task_id = 'import_to_postgres',
	postgres_conn_id = 'postgres_amazon',
	sql = "COPY amazon.amazon_purchases FROM '/Users/asamra/dev/DataEngineering.Labs.AirflowProject/data_amazon/amazon_purchases.csv' DELIMITER ',' CSV HEADER;",
	dag = dag,
	)


t1 >> [t2, t3]
