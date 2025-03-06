from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import timedelta
from etl_warehouse.tasks.extract import Extract

import pandas as pd
import pytz
import requests

class Transform:
    """
    A class used to transform data.
    """

    @staticmethod
    def _dim_products(table_name: str, table_extract:list, incremental:bool, **kwargs) -> pd.DataFrame:
        df_products = Extract._db(schema='public', table_name='products', incremental=incremental, **kwargs)

        if df_products.empty:
            raise AirflowSkipException(f"Dataframe for 'products' is empty. Skipped...")
        else:
            selected_columns = ['product_id', 'product_name']
            rename_columns = {
                'product_id': 'product_nk'
            }
            dim_products = df_products[selected_columns].rename(columns=rename_columns)

            # add created_at and updated_at columns
            dim_products['created_at'] = pd.Timestamp.now()
            dim_products['updated_at'] = pd.Timestamp.now()

            return dim_products
        
    @staticmethod
    def _dim_users(table_name: str, table_extract: list, incremental: bool, **kwargs) -> pd.DataFrame:
        df_users = Extract._db(schema='public', table_name='website_sessions', incremental=incremental, **kwargs)
        
        if df_users.empty:
            raise AirflowSkipException("Dataframe for 'website_sessions' is empty. Skipped...")
        else:
            # get distinct user_id
            dim_users = df_users[['user_id']].drop_duplicates().copy()
            dim_users['created_at'] = pd.Timestamp.now()
            dim_users['updated_at'] = pd.Timestamp.now()
            
            return dim_users

    @staticmethod
    def _fact_sessions(table_name: str, table_extract: list, incremental: bool, **kwargs) -> pd.DataFrame:
        df_sessions = Extract._db(schema='public', table_name='website_sessions', incremental=incremental, **kwargs)
        
        if df_sessions.empty:
            raise AirflowSkipException("Dataframe for 'website_sessions' is empty. Skipped...")
        else:
            dim_date = Extract._dwh(schema='public', table_name='dim_date', **kwargs)
            dim_time = Extract._dwh(schema='public', table_name='dim_time', **kwargs)
            
            df_sessions['date_id'] = df_sessions['created_at'].dt.date.map(dim_date.set_index('date_actual')['date_id'])
            df_sessions['time_id'] = df_sessions['created_at'].dt.floor('min').dt.time.map(dim_time.set_index('time_actual')['time_id'])

            df_sessions['is_repeat_session'] = df_sessions['is_repeat_session'].astype(bool)
            
            fact_sessions = df_sessions.rename(columns={'website_session_id': 'session_nk'})
            fact_sessions['created_at'] = pd.Timestamp.now()
            fact_sessions['updated_at'] = pd.Timestamp.now()
            
            return fact_sessions
    
    @staticmethod
    def _fact_pageviews(table_name: str, table_extract: list, incremental: bool, **kwargs) -> pd.DataFrame:
        df_pageviews = Extract._db(schema='public', table_name='website_pageviews', incremental=incremental, **kwargs)
    
        if df_pageviews.empty:
            raise AirflowSkipException("Dataframe for 'website_pageviews' is empty. Skipped...")
        else:        
            dim_date = Extract._dwh(schema='public', table_name='dim_date', **kwargs)
            dim_time = Extract._dwh(schema='public', table_name='dim_time', **kwargs)
            fact_sessions = Extract._dwh(schema='public', table_name='fact_sessions', **kwargs)

            df_pageviews['date_id'] = df_pageviews['created_at'].dt.date.map(dim_date.set_index('date_actual')['date_id'])
            df_pageviews['time_id'] = df_pageviews['created_at'].dt.floor('min').dt.time.map(dim_time.set_index('time_actual')['time_id'])

            # Merge df_pageviews dengan fact_sessions untuk mendapatkan session_id yang benar
            df_pageviews = df_pageviews.merge(
                fact_sessions[['session_nk', 'session_id']],
                left_on='website_session_id',
                right_on='session_nk',
                how='left'
            )

            # Drop COLUMN
            df_pageviews.drop(columns=['session_nk','website_session_id'], inplace=True)

            fact_pageviews = df_pageviews.rename(columns={'website_pageview_id': 'pageview_nk'})
            fact_pageviews['created_at'] = pd.Timestamp.now()
            fact_pageviews['updated_at'] = pd.Timestamp.now()

            return fact_pageviews
    
    @staticmethod
    def _fact_order_items(table_name: str, table_extract: list, incremental: bool, **kwargs) -> pd.DataFrame:
        df_orders = Extract._db(schema='public', table_name='orders', incremental=incremental, **kwargs)
        df_order_items = Extract._db(schema='public', table_name='order_items', incremental=incremental, **kwargs)

        
        if df_orders.empty or df_order_items.empty:
            raise AirflowSkipException("Dataframe for 'orders' or 'order_items' is empty. Skipped...")
        else:        
            dim_date = Extract._dwh(schema='public', table_name='dim_date', **kwargs)
            dim_time = Extract._dwh(schema='public', table_name='dim_time', **kwargs)
            fact_sessions = Extract._dwh(schema='public', table_name='fact_sessions', **kwargs)
            dim_product = Extract._dwh(schema='public', table_name='dim_products', **kwargs)
            
            fact_orders = df_orders.merge(df_order_items, on='order_id', how='inner')
            fact_orders['order_date'] = fact_orders['created_at_x'].dt.date.map(dim_date.set_index('date_actual')['date_id'])
            fact_orders['order_time'] = fact_orders['created_at_x'].dt.floor('min').dt.time.map(dim_time.set_index('time_actual')['time_id'])
            
            rename_columns = {
                'order_item_id': 'order_item_nk',
                'order_id': 'order_nk',
                'price_usd_y': 'item_price_usd',
                'cogs_usd_y': 'item_cogs_usd',
                'price_usd_x': 'order_price_usd',
                'cogs_usd_x': 'order_cogs_usd'
            }
            fact_orders = fact_orders.rename(columns=rename_columns)
            fact_orders['is_primary_item'] = fact_orders['is_primary_item'].astype(bool)

            fact_orders = fact_orders.merge(
                fact_sessions[['session_nk', 'session_id']],
                left_on='website_session_id',
                right_on='session_nk',
                how='left'
            )
            fact_orders = fact_orders.merge(
                dim_product[['product_nk', 'product_id']],
                left_on='product_id',
                right_on='product_nk',
                how='left'
            )
            fact_orders.drop(columns=['session_nk','website_session_id','created_at_x','created_at_y','product_nk','product_id_x'], inplace=True)

            fact_orders = fact_orders.rename(columns={'product_id_y': 'product_id'})

            # add created_at and updated_at columns
            fact_orders['created_at'] = pd.Timestamp.now()
            fact_orders['updated_at'] = pd.Timestamp.now()
            
            return fact_orders