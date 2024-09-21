from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'retail_sales_data_pipeline',
    default_args=default_args,
    description='Retail sales data pipeline using Dataflow and BigQuery',
    schedule_interval='@daily',  # Run daily
)

# Define the task to trigger the Dataflow job
run_dataflow = BashOperator(
    task_id='run_dataflow_pipeline',
    bash_command='python /beam/retail_sales_pipeline.py',
    dag=dag,
)

# Define the task sequence
run_dataflow
