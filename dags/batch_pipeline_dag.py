import os
from datetime import datetime
from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor
from csv_mysql_operator import CSVToMySQLOperator
from utils import DBConfig

from dotenv import load_dotenv

load_dotenv()

dag_file_path = os.path.abspath(__file__)
dag_directory = os.path.dirname(dag_file_path)

FILE_PATH = '../data/sample.csv'
LOCAL_DATA_CONN_ID = 'local_data_conn'
MYSQL_DATA_CONN_ID = 'manhattan_conn_id'


@dag(
    "batch_load_csv_to_postgres",
    start_date=datetime(2023, 7, 19),
    description="""
    Airflow DAG to batch load data from csv file locally to postgres
    """,
    schedule='@daily',
    catchup=False,
)
def batch_load_csv_to_postgres():
    """
      This functions creats a dag to load data from a local csv file if avaialble to a a postgres DB
    """

    is_source_data_available = FileSensor(
        task_id="is_source_data_available",
        fs_conn_id=LOCAL_DATA_CONN_ID,
        filepath=FILE_PATH,
        poke_interval=5,
        timeout=20
    )

    load_csv_to_mysql = CSVToMySQLOperator(
        task_id='load_csv_to_mysql',
        mysql_conn_id=MYSQL_DATA_CONN_ID,
        csv_file_path='data/sample.csv',
        table_name=f"staging.sample",
    )

    is_source_data_available >> load_csv_to_mysql


batch_load_csv_to_postgres()
