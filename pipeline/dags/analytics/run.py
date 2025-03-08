from airflow.decorators import dag
from airflow.models import Variable
from pendulum import datetime
from analytics.tasks.main import main
from helper.callbacks.slack_notifier import slack_notifier
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='user_segmentation',
    description='User segmentation and analytics pipeline',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def ecommerce_analytics_pipeline():
    """
    DAG to perform e-commerce user segmentation and analytics.
    
    This DAG:
    1. Extracts order and user data from the data warehouse
    2. Creates features for customer segmentation
    3. Segments users using K-means clustering
    4. Analyzes each segment's characteristics
    5. Loads the results back to the data warehouse
    """
         # Define the task to trigger the next DAG
    trigger_to_reverse = TriggerDagRunOperator(
        task_id='trigger_to_reverse',
        trigger_dag_id="reverse_etl",
        trigger_rule="none_failed"
    )
    # Create the main task group with ETL tasks
    main() >> trigger_to_reverse

# Instantiate the DAG
dag = ecommerce_analytics_pipeline()