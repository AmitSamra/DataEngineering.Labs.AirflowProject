import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import csv
import requests
import os
from airflow.operators.postgres_operator import PostgresOperator
import numpy as np
import pandas as pd


default_args = {
	'owner':'Mr. Amit',
	#'start_date': datetime(2020,4,5,0),
	'start_date': datetime.now(),
	'retries':0,
	'retry_delay':timedelta(minutes=1)
}


dag = DAG(
	'amazon',
	default_args = default_args,
	description = 'amazon',
	#schedule_interval = timedelta(hours=1),
	catchup = False,
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

def etl_csv():
	"""
	Cleans CSV & exports it as amazon_purchases_2.csv which will be read into Postgres database
	"""
	df = pd.read_csv('amazon_purchases.csv', parse_dates=['Order Date', 'Shipment Date'])
	df.columns = ['order_id', 'order_date', 'category', 'website', 'condition', 'seller', 'list_price_per_unit', 'purchase_price_per_unit', 'quantity', 'shipment_date', 'carrier_name', 'item_subtotal', 'item_subtotal_tax','item_total']
	df.category.fillna('', inplace = True)
	df.condition.fillna('', inplace = True)
	df.carrier_name.fillna('', inplace = True)
	df['list_price_per_unit'] = df['list_price_per_unit'].str.replace('$','').str.replace(',','')
	df['purchase_price_per_unit'] = df['purchase_price_per_unit'].str.replace('$','').str.replace(',','')
	df['item_subtotal'] = df['item_subtotal'].str.replace('$','').str.replace(',','')
	df['item_subtotal_tax'] = df['item_subtotal_tax'].str.replace('$','').str.replace(',','')
	df['item_total'] = df['item_total'].str.replace('$','').str.replace(',','')
	df['list_price_per_unit'] = df['list_price_per_unit'].astype(float)
	df['purchase_price_per_unit'] = df['purchase_price_per_unit'].astype(float)
	df['item_subtotal'] = df['item_subtotal'].astype(float)
	df['item_subtotal_tax'] = df['item_subtotal_tax'].astype(float)
	df['item_total'] = df['item_total'].astype(float)
	df = df[df.list_price_per_unit != 0]
	df = df[df.purchase_price_per_unit != 0]
	df = df[df.quantity != 0]
	df = df[df.item_subtotal != 0]
	df = df[df.item_total != 0]
	df['carrier_name'] = df['carrier_name'].replace('FEDEX','FedEx')
	df['carrier_name'] = df['carrier_name'].replace('SMARTPOST','FedEx Smartpost ')
	df['carrier_name'] = df['carrier_name'].replace('Mail Innovations','UPS Mail Innovations')
	df['carrier_name'] = df['carrier_name'].replace('UPS MI','UPS Mail Innovations')
	df['carrier_name'] = df['carrier_name'].replace('US Postal Service','USPS')
	df.to_csv('amazon_purchases_2.csv', index=False)


t2 = PythonOperator(
	task_id = 'etl_csv',
	python_callable = etl_csv,
	provide_context = False,
	dag = dag,
	)


t3 = PostgresOperator(
	task_id = 'create_table',
	postgres_conn_id = 'postgres_amazon',
	sql = '''CREATE TABLE IF NOT EXISTS amazon.amazon_purchases(
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


path = os.path.join(os.path.dirname(__file__),'../amazon_purchases_2.csv')

t4 = PostgresOperator(
	task_id = 'import_to_postgres',
	postgres_conn_id = 'postgres_amazon',
	sql = f"DELETE FROM amazon.amazon_purchases; COPY amazon.amazon_purchases FROM '{path}' DELIMITER ',' CSV HEADER;",
	dag = dag,
	)


t1 >> t2 >> t3 >> t4