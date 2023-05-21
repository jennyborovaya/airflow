#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_domain_10():
    top_domain_10 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_10['domain_10'] = top_domain_10['domain'].apply(lambda x: x.split('.')[-1])
    top_domain_10 = top_domain_10.groupby('domain_10',as_index = False).agg({'domain' : 'count'}).rename(columns={'domain' : 'count'}).sort_values('count', ascending = False).head(10)
    
    with open('top_domain_10.csv', 'w') as f:
        f.write(top_domain_10.to_csv(index=False, header=False))

def top_domain_max_len():
    top_domain_max_len = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_max_len['len_domain'] = top_domain_max_len.domain.str.len()
    top_domain_max_len = top_domain_max_len.sort_values('len_domain', ascending = False).head(1)
    
    with open('top_domain_max_len.csv', 'w') as f:
        f.write(top_domain_max_len.to_csv(index=False, header=False))

def domain_airflow():
    domain_airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_airflow = domain_airflow.query("domain == 'airflow.com'")[['rank']]
    
    with open('domain_airflow.csv', 'w') as f:
        f.write(domain_airflow.to_csv(index=False, header=False))


def print_a(ds):
    with open('top_domain_10.csv', 'r') as f:
        top_domain_10 = f.read()
    with open('top_domain_max_len.csv', 'r') as f:
        top_domain_max_len = f.read()
    with open('domain_airflow.csv', 'r') as f:
        domain_airflow = f.read()
    date = ds

    print(f'Top domains date {date}')
    print(top_domain_10)

    print(f'Max len domain date {date}')
    print(top_domain_max_len)
    
    print(f'Domain airflow date {date}')
    print(domain_airflow)


default_args = {
    'owner': 'e-borovaja-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 13),
}
schedule_interval = '0 6 * * *'

dag = DAG('e-borovaja-31', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domain_10',
                    python_callable=top_domain_10,
                    dag=dag)

t3 = PythonOperator(task_id='top_domain_max_len',
                        python_callable=top_domain_max_len,
                        dag=dag)

t4 = PythonOperator(task_id='domain_airflow',
                    python_callable=domain_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_a',
                    python_callable=print_a,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

