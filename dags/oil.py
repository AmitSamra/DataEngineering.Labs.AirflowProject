from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

default_args = {
	'owner':'Mr. Amit',
	'start_date': datetime.now(),
	'retries':2,
	'retry_delay':timedelta(minutes=1)
}

dag = DAG(
	'ex1',
	default_args = default_args,
	description = 'Oily Money',
	#schedule_interval=timedelta(hours=1)
	)

def read_csv():
