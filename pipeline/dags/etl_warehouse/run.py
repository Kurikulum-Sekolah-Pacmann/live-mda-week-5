from airflow.decorators import dag
from airflow.models import Variable
from pendulum import datetime
from etl_warehouse.tasks.main import main_etl
from helper.callbacks.slack_notifier import slack_notifier

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='etl_warehouse',
    description='Extract data, transform and load into warehouse area',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def etl_warehouse():
    """
    DAG function to Extract data, transform and load into warehouse area.
    """
    # Get the incremental mode from Airflow Variables
    incremental_mode = Variable.get('INCREMENTAL_MODE', default_var='True')
    incremental_mode = eval(incremental_mode)  # Convert string to boolean

    # Create the main task group with ETL tasks
    main_etl(incremental=incremental_mode)

# Instantiate the DAG
dag = etl_warehouse()