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
                    *
                FROM dim_user_segments us
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