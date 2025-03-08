from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from reverse_etl.tasks.extract import Extract
from reverse_etl.tasks.transform import Transform
from reverse_etl.tasks.load import Load
import pandas as pd

def extract_users(**context):
    """
    Extract user segment data
    """
    df = Extract.extract_user_segments(**context)
    
    if df is not None:
        return df.to_dict(orient='records')
    return None

def extract_products(**context):
    """
    Extract product data
    """
    df = Extract.extract_products(**context)
    
    if df is not None:
        # Convert DataFrame to dictionary for XCom serialization
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and not pd.isna(x) else (None if pd.isna(x) else x))
        return df.to_dict(orient='records')
    return None

def transform_users(**context):
    """
    Transform user data for Typesense
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="typesense_sync.extract_users")
    
    if data is None:
        return None
        
    # Convert dictionary back to DataFrame
    df = pd.DataFrame(data)
    
    # Transform data
    transformed_data = Transform.prepare_user_data(df, **context)
    
    return transformed_data

def transform_products(**context):
    """
    Transform product data for Typesense
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="typesense_sync.extract_products")
    
    if data is None:
        return None
        
    # Convert dictionary back to DataFrame
    df = pd.DataFrame(data)
    
    # Transform data
    transformed_data = Transform.prepare_product_data(df, **context)
    
    return transformed_data

def load_users(**context):
    """
    Load user data to Typesense
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="typesense_sync.transform_users")
    
    if data is None or len(data) == 0:
        return None
        
    # Load data to Typesense
    Load.load_users_to_typesense(data, **context)

def load_products(**context):
    """
    Load product data to Typesense
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="typesense_sync.transform_products")
    
    if data is None or len(data) == 0:
        return None
        
    # Load data to Typesense
    Load.load_products_to_typesense(data, **context)

@task_group(group_id='typesense_sync')
def main():
    """
    Main task group for Typesense data sync
    """
    # Extract tasks
    extract_users_task = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users,
        provide_context=True
    )
    
    extract_products_task = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
        provide_context=True
    )
    
    # Transform tasks
    transform_users_task = PythonOperator(
        task_id="transform_users",
        python_callable=transform_users,
        provide_context=True
    )
    
    transform_products_task = PythonOperator(
        task_id="transform_products",
        python_callable=transform_products,
        provide_context=True
    )
    
    # Load tasks
    load_users_task = PythonOperator(
        task_id="load_users_to_typesense",
        python_callable=load_users,
        provide_context=True
    )
    
    load_products_task = PythonOperator(
        task_id="load_products_to_typesense",
        python_callable=load_products,
        provide_context=True
    )
    
    # Define task dependencies for the user pipeline
    extract_users_task >> transform_users_task >> load_users_task
    
    # Define task dependencies for the product pipeline
    extract_products_task >> transform_products_task >> load_products_task