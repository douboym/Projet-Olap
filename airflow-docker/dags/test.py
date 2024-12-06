from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def test_collect_data():
    import requests
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/subventions-associations-votees-/records?limit=10&offset=0"
    response = requests.get(url)
    print(f"Statut HTTP : {response.status_code}")
    print(response.json())

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG('test_collect_data_dag', default_args=default_args, schedule_interval=None) as dag:
    task = PythonOperator(
        task_id='test_collect_data',
        python_callable=test_collect_data,
    )

