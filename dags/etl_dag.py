import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from boto3 import client
from botocore.config import Config
from operators import DataSourceToCsv

# Path configs
TEMP_FILES = './'

DEFAULT_ARGS = {
    'owner': 'eroshkinas',
    'start_date': datetime.now() - timedelta(days=1),
    'execution_timeout': timedelta(hours=1),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 3,
}

def create_connection():
    try:
        db_con = BaseHook.get_connection('skyeng_db')
        return db_con.connect(db_con.host)
    except Exception as e:
        logging.error(e)
    return None

    
with DAG(
    dag_id='data_import',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
) as dag:



    dag_start = DummyOperator(
    task_id='skyeng_etl_start'
    )

    extract_task = PostgresOperator(
        task_id='read_table',
        postgres_conn_id='skyeng_db',
        sql=''' SELECT * FROM etl.order; ''',
    )

    extract_task = DataSourceToCsv.DataSourceToCsv(
        task_id='extract_table',
        table_name = 'tablename',
        extract_query = ''' SELECT * 
                            FROM etl.order
                            WHERE updated_at > $EXECUTION_DATE; ''',
        connection = 'skyeng_db')


    dag_start >> extract_task




