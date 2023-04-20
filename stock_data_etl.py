from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from yahoo_stock_data import get_stock_data
from google.cloud import bigquery
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule=None, start_date=days_ago(2))

def stock_data_etl():
    @task()
    def extract():
        return get_stock_data("msft", "2022-04-20", "2022-04-21")

    @task()
    def transform(stock_data: pd.DataFrame) -> pd.DataFrame:
        return stock_data

    @task()
    def load(transformed_stock_data: pd.DataFrame) -> None:
        # Construct a BigQuery client object.
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()

        table_id = "stock_data_microsoft"

        job = client.load_table_from_dataframe(
        transformed_stock_data, table_id, job_config=job_config
        )  # Make an API request.

        job.result()  # Wait for the job to complete.


    stock_data = extract()

    transformed_stock_data = transform(stock_data)

    load(transformed_stock_data)

stock_data_etl_dag = stock_data_etl()
