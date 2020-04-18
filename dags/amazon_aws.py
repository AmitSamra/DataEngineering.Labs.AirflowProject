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
from airflow.operators.papermill_operator import PapermillOperator
import airflow.hooks.S3_hook
import airflow.hooks.postgres_hook
from dotenv import load_dotenv


dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True) 


default_args = {
	'owner':'Mr. Amit',
	'start_date': datetime(2020,4,10,0),
	#'start_date': datetime.now(),
	'retries':0,
	'retry_delay':timedelta(minutes=1)
}


dag = DAG(
	'amazon_aws',
	default_args = default_args,
	description = 'amazon',
	#schedule_interval = timedelta(hours=1),
	catchup = False,
	max_active_runs = 1,
	)


def get_amazon_purchases():
	"""
	Gets my Amazon order history for 12 years
	"""
	url = 'https://airflowfiles.s3.amazonaws.com/amazon_purchases.csv'
	response = requests.get(url)
	path = os.path.join(os.path.dirname(__file__),'../amazon_purchases.csv')
	with open(path, 'wb') as f:
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
	path = os.path.join(os.path.dirname(__file__),'../amazon_purchases.csv')
	path2 = os.path.join(os.path.dirname(__file__),'../amazon_purchases_2.csv')
	df = pd.read_csv(path, parse_dates=['Order Date', 'Shipment Date'])
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
	df['category'] = df['category'].replace(['NOTEBOOK_COMPUTER','COMPUTER_DRIVE_OR_STORAGE','RAM_MEMORY','TABLET_COMPUTER','MONITOR','COMPUTER_COMPONENT', 'FLASH_MEMORY', 'SOFTWARE', 'INK_OR_TONER', 'COMPUTER_INPUT_DEVICE', 'CABLE_OR_ADAPTER', 'NETWORKING_DEVICE', 'KEYBOARDS', 'COMPUTER_ADD_ON', 'NETWORKING_ROUTER','MEMORY_READER','WIRELESS_ACCESSORY','SCANNER','PRINTER'],'COMPUTER')
	df['category'] = df['category'].replace(['HEADPHONES','SPEAKERS','BATTERY','MULTIFUNCTION_DEVICE','ELECTRONIC_CABLE','SURVEILANCE_SYSTEMS','SECURITY_CAMERA','WATCH','CONSUMER_ELECTRONICS','CE_ACCESSORY','ELECTRONIC_ADAPTER','ELECTRIC_FAN','CAMCORDER','HANDHELD_OR_PDA','TUNER','AMAZON_BOOK_READER','CELLULAR_PHONE','POWER_SUPPLIES_OR_PROTECTION','CAMERA_OTHER_ACCESSORIES','CHARGING_ADAPTER'],'ELECTRONICS')
	df['category'] = df['category'].replace(['HAIR_STYLING_AGENT','PERSONAL_CARE_APPLIANCE','PROFESSIONAL_HEALTHCARE','HEALTH_PERSONAL_CARE','SHAMPOO','VITAMIN','ABIS_DRUGSTORE','BEAUTY'],'HEALTH_BEAUTY')
	df['category'] = df['category'].replace(['KITCHEN','SEEDS_AND_PLANTS','HOME_LIGHTING_ACCESSORY','BOTTLE','OUTDOOR_LIVING','ELECTRIC_FAN','TABLECLOTH','COFFEE_MAKER','HOME_BED_AND_BATH','HOME_LIGHTING_AND_LAMPS','SMALL_HOME_APPLIANCES'],'HOME')
	df['category'] = df['category'].replace(['SHOES','PANTS','SHIRT','SHORTS','OUTERWEAR','SWEATSHIRT','HAT', 'SOCKSHOSIERY','UNDERWEAR','TECHNICAL_SPORT_SHOE'],'APPAREL')
	df['category'] = df['category'].replace(['OUTDOOR_RECREATION_PRODUCT','SPORTING_GOODS'],'SPORTS_OUTDOOR')
	df['category'] = df['category'].replace(['TEA','COFFEE'],'GROCERY')
	df['category'] = df['category'].replace(['AUTO_PART','HARDWARE','AUTO_ACESSORY','PRECISION_MEASURING','BUILDING_MATERIAL','AUTO_ACCESSORY'],'TOOLS')
	df['category'] = df['category'].replace(['WRITING_INSTRUMENT','PAPER_PRODUCT','BACKPACK','CARRYING_CASE_OR_BAG','CE_CARRYING_CASE_OR_BAG','OFFICE_PRODUCTS'],'OFFICE')
	df['category'] = df['category'].replace(['ABIS_DVD','TOYS_AND_GAMES','ABIS_MUSIC','DOWNLOADABLE_VIDEO_GAME','ART_AND_CRAFT_SUPPLY'],'ENTERTAINMENT')
	df['category'] = df['category'].replace(['ABIS_BOOK'],'BOOKS')
	df['category'] = df['category'].replace(['ABIS_GIFT_CARD'],'GIFT_CARD')
	df['category'] = df['category'].replace(['AV_FURNITURE','CELLULAR_PHONE_CASE','PHONE_ACCESSORY','PET_SUPPLIES','ACCESSORY','BAG','ACCESSORY_OR_PART_OR_SUPPLY'],'OTHER')
	df['category'] = df['category'].replace([''],'NONE_MISSING')
	df.to_csv(path2, index=False)


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
hook_copy_expert = airflow.hooks.postgres_hook.PostgresHook('postgres_amazon')

def import_to_postgres():
	"""
	Imports CSV into Postgres RDS
	"""
	sql = "DELETE FROM amazon.amazon_purchases; COPY amazon.amazon_purchases FROM STDIN WITH CSV DELIMITER ',' HEADER;"
	hook_copy_expert.copy_expert(sql, path, open=open)


t4 = PythonOperator(
	task_id = 'import_to_postgres',
	python_callable = import_to_postgres,
	dag = dag,
	)


path2 = os.path.join(os.path.dirname(__file__),'../amazon_visualization_aws.ipynb')
#path3 = os.path.join(os.path.dirname(__file__),'../amazon_visualization_aws{{ execution_date }}.ipynb')
path3 = os.path.join(os.path.dirname(__file__),'../amazon_visualization_aws_output.ipynb')


t5 = BashOperator(
	task_id = 'run_notebook',
	#bash_command = 'jupyter nbconvert --to notebook --execute {}'.format(path2) + ' --output amazon_visualization_aws_output.ipynb --notebook-dir /home/amit/airflow',
	bash_command = 'export TMPDIR=/home/amit/airflow; jupyter nbconvert --to notebook --execute /home/amit/airflow/amazon_visualization_aws.ipynb --output amazon_visualization_aws_output.ipynb --ExecutePreprocessor.kernel_name=python3',
	dag = dag,
	)


path4 = os.path.join(os.path.dirname(__file__),'../amazon_orders.html')
hook_s3 = airflow.hooks.S3_hook.S3Hook('airflow_aws_s3')


def load_files_to_S3():
	"""
	Saves files to S3
	"""
	hook_s3.load_file(path,'amazon_purchases_2.csv', 'airflowfiles', replace=True)
	hook_s3.load_file(path2,'amazon_visualization_aws.ipynb', 'airflowfiles', replace=True)
	hook_s3.load_file(path3,'amazon_visualization_aws_output.ipynb', 'airflowfiles', replace=True)
	hook_s3.load_file(path4,'html_files/amazon_orders.html', 'airflowfiles', replace=True)

	directory = os.path.join(os.path.dirname(__file__), '../')
	pngs = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".png")]

	for f in pngs:
		hook_s3.load_file(f, 'html_files/' + f.split('/')[-1], 'airflowfiles', replace = True)


t6 = PythonOperator(
	task_id = 'load_files_to_S3',
	python_callable = load_files_to_S3,
	provide_context = False,
	dag = dag,
	)



t1 >> t2 >> t3 >> t4 >> t5 >> t6

