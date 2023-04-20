from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from yahoo_stock_data import get_stock_data
from google.cloud import bigquery
import pandas as pd
import logging

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

company_name = "msft"

@dag(default_args=default_args, schedule=None, start_date=days_ago(2))

def stock_data_etl():
    @task()
    def extract():

        start_date = "2022-04-20"
        end_date = "2022-04-21"

        data = get_stock_data(company_name, start_date, end_date)

        logging.info(f"Extracted stock data for company, {company_name} for period [{start_date}, {end_date}]...")

        return data

    @task()
    def transform(stock_data: pd.DataFrame) -> pd.DataFrame:

        transformed = stock_data

        logging.info("Transformed stock data...")

        return transformed

    @task()
    def load(transformed_stock_data: pd.DataFrame) -> None:
        # Construct a BigQuery client object.
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()

        project_name = "conference-demo-384318"
        
        dataset_name = "yahoo_finance_data"

        table_id = f"{project_name}.{dataset_name}.stock_data_{company_name}"

        job = client.load_table_from_dataframe(
        transformed_stock_data, table_id, job_config=job_config
        )  # Make an API request.

        job.result()  # Wait for the job to complete.

        logging.info(f"Loaded stock data into BigQuery table of ID, {table_id}.")


    stock_data = extract()

    transformed_stock_data = transform(stock_data)

    load(transformed_stock_data)

stock_data_etl_dag = stock_data_etl()
