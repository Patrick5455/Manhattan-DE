import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import mysql.connector


class CSVToMySQLOperator(BaseOperator):
    """
    Custom Airflow operator to load CSV data into MySQL.

    :param mysql_conn_id: The Airflow connection ID for MySQL.
    :type mysql_conn_id: str
    :param csv_file_path: The local path to the CSV file.
    :type csv_file_path: str
    :param table_name: The name of the target MySQL table.
    :type table_name: str
    """

    @apply_defaults
    def __init__(self, mysql_conn_id, csv_file_path, table_name, *args, **kwargs):
        super(CSVToMySQLOperator, self).__init__(*args, **kwargs)

        self.mysql_conn_id = mysql_conn_id
        self.csv_file_path = csv_file_path
        self.table_name = table_name

    def execute(self, context):

        df = pd.read_csv(self.csv_file_path)

        mysql_conn = BaseHook.get_connection(self.mysql_conn_id)

        mysql_conn = mysql.connector.connect(
            user=mysql_conn.login,
            password=mysql_conn.password,
            host=mysql_conn.host,
            port=mysql_conn.port
        )

        print("connection successful")

        cursor = mysql_conn.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
        for column in df.columns:
            create_table_query += f"{column} VARCHAR(255), "
        create_table_query = create_table_query.rstrip(", ") + ");"
        print(f"create_table_query: {create_table_query}")
        cursor.execute(create_table_query)

        for _, row in df.iterrows():
            insert_query = f"INSERT INTO {self.table_name} VALUES ("
            for value in row.values:
                insert_query += f"'{value}', "
            insert_query = insert_query.rstrip(", ") + ");"
            print(f"insert_query: {insert_query}")
            cursor.execute(insert_query)
        mysql_conn.commit()
        cursor.close()
        mysql_conn.close()
