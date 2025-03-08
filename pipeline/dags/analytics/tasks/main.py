from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from analytics.tasks.extract import Extract
from analytics.tasks.analytics import Analytics
from analytics.tasks.load import Load
import pandas as pd
import numpy as np

def extract_data(**context):
    """
    Extract data from the data warehouse.
    """
    query = """
        SELECT 
            foi.user_id,
            foi.order_nk,
            foi.product_id,
            foi.is_primary_item,
            foi.item_price_usd,
            foi.item_cogs_usd,
            dd.date_actual,
            foi.items_purchased,
            foi.order_price_usd,
            foi.order_cogs_usd,
            dp.product_name
        FROM fact_order_items foi
        JOIN dim_products dp ON foi.product_id = dp.product_id
        JOIN dim_users du ON foi.user_id = du.user_id
        JOIN dim_date dd ON foi.order_date = dd.date_id;
        """
    
    df = Extract.extract_data(query=query, **context)
    
    if df is not None:
        # Convert DataFrame to dictionary for XCom serialization
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and not pd.isna(x) else (None if pd.isna(x) else x))
        return df.to_dict(orient='records')
    return None

def create_features(**context):
    """
    Create features for segmentation.
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="analytics.extract_warehouse_data")
    
    if data is None:
        return None
        
    # Convert dictionary back to DataFrame
    df = pd.DataFrame(data)
    
    features_df = Analytics.create_features(df=df, **context)
    
    if features_df is not None:
        # Convert DataFrame to dictionary for XCom serialization
        features_df = features_df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and not pd.isna(x) else (None if pd.isna(x) else x))
        return features_df.to_dict(orient='records')
    return None

def segment_users(**context):
    """
    Segment users using clustering.
    """
    # Get number of clusters from Airflow Variables
    n_clusters = int(Variable.get("N_CLUSTER"))
    
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="analytics.create_customer_features")
    
    if data is None:
        return None
        
    # Convert dictionary back to DataFrame
    features_df = pd.DataFrame(data)
    
    # Perform segmentation
    segmented_df = Analytics.segment_users(features=features_df, n_clusters=n_clusters, **context)
    
    if segmented_df is not None:
        # Define a function to format each value in the DataFrame
        def format_value(x):
            if isinstance(x, pd.Timestamp) and not pd.isna(x):
                return x.isoformat()
            elif isinstance(x, (list, np.ndarray)):  # Handle lists or arrays
                return [format_value(item) for item in x]
            elif pd.isna(x):  # Handle NaN values
                return None
            else:
                return x
        
        # Apply the formatting function to each element in the DataFrame
        segmented_df = segmented_df.applymap(format_value)
        
        # Convert DataFrame to dictionary for XCom serialization
        return segmented_df.to_dict(orient='records')
    return None

def analyze_segments(**context):
    """
    Analyze the user segments.
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids="analytics.segment_users")
    
    if data is None:
        return None
        
    # Convert dictionary back to DataFrame
    segmented_df = pd.DataFrame(data)
    
    analysis_df = Analytics.analyze_segments(features=segmented_df, **context)
    
    if analysis_df is not None:
        # # Convert DataFrame to dictionary for XCom serialization
        # analysis_df = analysis_df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and not pd.isna(x) else (None if pd.isna(x) else x))
        return analysis_df.to_dict(orient='records')
    return None

def load_data(**context):
    """
    Load results to the data warehouse.
    """
    # Get data from previous tasks
    ti = context['task_instance']
    # segmented_data = ti.xcom_pull(task_ids="analytics.segment_users")
    analysis_data = ti.xcom_pull(task_ids="analytics.analyze_segments")
    
    analysis_df = pd.DataFrame(analysis_data)
    
    Load.save_results(features=analysis_df, **context)

@task_group(group_id='analytics')
def main():
    """
    Main task group for Analytics Pipeline process.
    """
    # Extract data
    extract_task = PythonOperator(
        task_id="extract_warehouse_data",
        python_callable=extract_data,
        provide_context=True
    )
    
    # Create features
    create_features_task = PythonOperator(
        task_id="create_customer_features",
        python_callable=create_features,
        provide_context=True
    )
    
    # Segment users
    segment_users_task = PythonOperator(
        task_id="segment_users",
        python_callable=segment_users,
        provide_context=True
    )
    
    # Analyze segments
    analyze_segments_task = PythonOperator(
        task_id="analyze_segments",
        python_callable=analyze_segments,
        provide_context=True
    )
    
    # Load data
    load_task = PythonOperator(
        task_id="save_results_to_warehouse",
        python_callable=load_data,
        provide_context=True
    )
    
    # Define task dependencies
    extract_task >> create_features_task >> segment_users_task >> analyze_segments_task >> load_task