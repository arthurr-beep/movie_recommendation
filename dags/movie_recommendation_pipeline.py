from datetime import timedelta
from subprocess import call
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def run_kafka_producer():
    call(["python", "/opt/airflow/dags/kafka_producer.py"])

def run_pyspark_consumer():
    call(["spark-submit", "/opt/airflow/dags/pyspark_consumer.py"])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'movie_recommendation_pipeline',
    default_args=default_args,
    description='A movie recommendation pipeline using Kafka and PySpark',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=run_kafka_producer,
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_pyspark_consumer',
    python_callable=run_pyspark_consumer,
    dag=dag,
)

t1 >> t2
