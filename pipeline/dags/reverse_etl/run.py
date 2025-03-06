from airflow.decorators import dag
from airflow.models import Variable
from pendulum import datetime
from reverse_etl.tasks.main import main
from helper.callbacks.slack_notifier import slack_notifier

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='reverse_etl',
    description='Extract data and load into typsense search engine',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def reverse_etl():
    """
    DAG function to Extract data and load into typsense search engine.
    """

    # Create the main task group with ETL tasks
    main()

# Instantiate the DAG
dag = reverse_etl()