import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import csv
import requests
import os


default_args = {
	'owner':'Mr. Amit',
	'start_date': datetime.now(),
	'retries':5,
	'retry_delay':timedelta(minutes=1)
}


dag = DAG(
	'oil',
	default_args = default_args,
	description = 'Oily Money',
	#schedule_interval=timedelta(hours=1)
	)


def get_unrate():
	"""
	Gets overall US unemployment rate by month
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=UNRATE&scale=left&cosd=1948-01-01&coed=2020-03-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1948-01-01https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=UNRATE&scale=left&cosd=1948-01-01&coed=2020-03-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1948-01-01'
	response = requests.get(url)
	with open('UNRATE.csv', 'wb') as f:
		f.write(response.content)


t1 = PythonOperator(
	task_id = 'get_unrate',
	python_callable = get_unrate,
	provide_context = False,
	dag = dag,
	)


def get_txur():
	"""
	Gets TX unemployment rate by month
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=TXUR&scale=left&cosd=1976-01-01&coed=2020-02-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1976-01-01'
	response = requests.get(url)
	with open('TXUR.csv', 'wb') as f:
		f.write(response.content)


t2 = PythonOperator(
	task_id = 'get_txur',
	python_callable = get_txur,
	provide_context = False,
	dag = dag,
	)


def get_caur():
	"""
	Gets CA unemployment rate by month
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CAUR&scale=left&cosd=1976-01-01&coed=2020-02-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1976-01-01'
	response = requests.get(url)
	with open('CAUR.csv', 'wb') as f:
		f.write(response.content)


t3 = PythonOperator(
	task_id = 'get_caur',
	python_callable = get_caur,
	provide_context = False,
	dag = dag,
	)


def get_ussthpi():
	"""
	Gets US House Price Index value by quarter
	1980 Q1 = 100
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=USSTHPI&scale=left&cosd=1975-01-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1975-01-01'
	response = requests.get(url)
	with open('USSTHPI.csv', 'wb') as f:
		f.write(response.content)


t4 = PythonOperator(
	task_id = 'get_ussthpi',
	python_callable = get_ussthpi,
	provide_context = False,
	dag = dag,
	)


def get_txsthpi():
	"""
	Gets TX House Price Index value by quarter
	1980 Q1 = 100
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=TXSTHPI&scale=left&cosd=1975-01-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1975-01-01'
	response = requests.get(url)
	with open('TXSTHPI.csv', 'wb') as f:
		f.write(response.content)


t5 = PythonOperator(
	task_id = 'get_txsthpi',
	python_callable = get_txsthpi,
	provide_context = False,
	dag = dag,
	)


def get_casthpi():
	"""
	Gets CA House Price Index value by quarter
	1980 Q1 = 100
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CASTHPI&scale=left&cosd=1975-01-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1975-01-01'
	response = requests.get(url)
	with open('CASTHPI.csv', 'wb') as f:
		f.write(response.content)


t6 = PythonOperator(
	task_id = 'get_casthpi',
	python_callable = get_casthpi,
	provide_context = False,
	dag = dag,
	)


def get_hxpi():
	"""
	Gets Houston House Price Index value by quarter
	1995 Q1 = 100
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CASTHPI&scale=left&cosd=1975-01-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1975-01-01'
	response = requests.get(url)
	with open('HXPI.csv', 'wb') as f:
		f.write(response.content)


t7 = PythonOperator(
	task_id = 'get_hxpi',
	python_callable = get_hxpi,
	provide_context = False,
	dag = dag,
	)






t1 >> [t2, t3, t4, t5, t6]















