from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.decorators import task
from airflow.models import Variable

from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
import pytz

class Extract:
    @staticmethod
    def extract_user_segments(**context):
        """
        Extract user segments from the data warehouse
        """
        try:
            # Get connection from PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id='warehouse')
            
            # Query to get user segments with their data
            query = """
                SELECT 
                    us.user_id,
                    us.segment,
                    s.user_count,
                    s.avg_total_orders,
                    s.avg_total_spent,
                    s.avg_avg_order_value,
                    s.avg_avg_item_price,
                    s.avg_total_items_purchased,
                    s.avg_time_since_first_order
                FROM dim_user_segments us
                JOIN dim_segments s ON us.segment = s.segment
            """
            
            df = postgres_hook.get_pandas_df(query)
            return df
            
        except Exception as e:
            raise Exception(f"Error extracting user segments: {str(e)}")
    
    @staticmethod
    def extract_products(**context):
        """
        Extract products data from the data warehouse
        """
        try:
            # Get connection from PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id='warehouse')
            
            # Query to get product data
            query = """
                SELECT 
                    p.product_id,
                    p.product_nk,
                    p.product_name,
                    p.created_at,
                    p.updated_at
                FROM dim_products p
            """
            
            df = postgres_hook.get_pandas_df(query)
            return df
            
        except Exception as e:
            raise Exception(f"Error extracting products: {str(e)}")