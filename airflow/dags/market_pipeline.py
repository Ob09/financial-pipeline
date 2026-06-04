from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def run_ingestion_task():
    import sys
    sys.path.insert(0, '/opt/airflow')
    from ingestion.fetch_stocks import run_ingestion
    run_ingestion()


default_args = {
    'owner':            'data_engineer',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id='market_data_pipeline',
    default_args=default_args,
    description='Daily financial market data pipeline',
    schedule_interval='0 6 * * 1-5',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'market_data'],
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=run_ingestion_task,
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='spark-submit /opt/spark/jobs/bronze_to_silver.py',
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='spark-submit /opt/spark/jobs/silver_to_gold.py',
    )

    notify_complete = PythonOperator(
        task_id='notify_complete',
        python_callable=lambda: logger.info(
            'Pipeline complete. Gold layer is ready.'
        ),
    )

    fetch_data >> bronze_to_silver >> silver_to_gold >> notify_complete