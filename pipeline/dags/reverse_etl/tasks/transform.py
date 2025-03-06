from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.decorators import task
from airflow.models import Variable

from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
from typing import List, Dict, Any

class Transform:
    @staticmethod
    def prepare_user_data(df: pd.DataFrame, **context) -> List[Dict[str, Any]]:
        """
        Transform user data for Typesense
        """
        if df is None or df.empty:
            return []
            
        # Convert to records
        users = []
        for _, row in df.iterrows():
            user = {
                'id': str(row['user_id']),
                'segment': row['segment'],
                'avg_order_value': float(row['avg_avg_order_value']),
                'total_spent': float(row['avg_total_spent'])
            }
            users.append(user)
            
        return users
    
    @staticmethod
    def prepare_product_data(df: pd.DataFrame, **context) -> List[Dict[str, Any]]:
        """
        Transform product data for Typesense
        """
        if df is None or df.empty:
            return []
            
        # Convert to records
        products = []
        for _, row in df.iterrows():
            product = {
                'id': str(row['product_id']),
                'name': row['product_name'],
                'created_at': row['created_at'] if pd.notna(row['created_at']) else None,
            }
            products.append(product)
            
        return products
