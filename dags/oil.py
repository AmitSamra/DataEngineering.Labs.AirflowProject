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


def get_sfpi():
	"""
	Gets San Francisco House Price Index value by quarter
	1995 Q1 = 100
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=ATNHPIUS41884Q&scale=left&cosd=1975-07-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1975-07-01'
	response = requests.get(url)
	with open('SFPI.csv', 'wb') as f:
		f.write(response.content)


t8 = PythonOperator(
	task_id = 'get_sfpi',
	python_callable = get_sfpi,
	provide_context = False,
	dag = dag,
	)


def get_uspop():
	"""
	Gets US population annual
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=POPTHM&scale=left&cosd=1959-01-01&coed=2020-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1959-01-01'
	response = requests.get(url)
	with open('USPOP.csv', 'wb') as f:
		f.write(response.content)


t9 = PythonOperator(
	task_id = 'get_uspop',
	python_callable = get_uspop,
	provide_context = False,
	dag = dag,
	)


def get_txpop():
	"""
	Gets TX population annual
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=TXPOP&scale=left&cosd=1900-01-01&coed=2019-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1900-01-01'
	response = requests.get(url)
	with open('TXPOP.csv', 'wb') as f:
		f.write(response.content)


t10 = PythonOperator(
	task_id = 'get_txpop',
	python_callable = get_txpop,
	provide_context = False,
	dag = dag,
	)


def get_capop():
	"""
	Gets CA population annual
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CAPOP&scale=left&cosd=1900-01-01&coed=2019-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1900-01-01'
	response = requests.get(url)
	with open('CAPOP.csv', 'wb') as f:
		f.write(response.content)


t11 = PythonOperator(
	task_id = 'get_capop',
	python_callable = get_capop,
	provide_context = False,
	dag = dag,
	)


def get_usgdp():
	"""
	Gets US GDP quarterly
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDPC1&scale=left&cosd=1947-01-01&coed=2019-10-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1947-01-01'
	response = requests.get(url)
	with open('USGDP.csv', 'wb') as f:
		f.write(response.content)


t12 = PythonOperator(
	task_id = 'get_usgdp',
	python_callable = get_usgdp,
	provide_context = False,
	dag = dag,
	)


def get_txgdp():
	"""
	Gets TX GDP annual
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=TXRGSP&scale=left&cosd=1997-01-01&coed=2018-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1997-01-01'
	response = requests.get(url)
	with open('TXGDP.csv', 'wb') as f:
		f.write(response.content)


t13 = PythonOperator(
	task_id = 'get_txgdp',
	python_callable = get_txgdp,
	provide_context = False,
	dag = dag,
	)


def get_cagdp():
	"""
	Gets CA GDP annual
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CARGSP&scale=left&cosd=1997-01-01&coed=2018-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1997-01-01'
	response = requests.get(url)
	with open('CAGDP.csv', 'wb') as f:
		f.write(response.content)


t14 = PythonOperator(
	task_id = 'get_cagdp',
	python_callable = get_cagdp,
	provide_context = False,
	dag = dag,
	)


def get_wtipi():
	"""
	Gets spot WTI crude oil price monthly
	"""
	url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=WTISPLC&scale=left&cosd=1946-01-01&coed=2020-02-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2020-04-03&revision_date=2020-04-03&nd=1946-01-01'
	response = requests.get(url)
	with open('wtipi.csv', 'wb') as f:
		f.write(response.content)


t15 = PythonOperator(
	task_id = 'get_wtipi',
	python_callable = get_wtipi,
	provide_context = False,
	dag = dag,
	)


t1 >> [t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15]















