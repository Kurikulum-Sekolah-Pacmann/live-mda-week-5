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
    def extract_data(query, **kwargs) -> pd.DataFrame:
        """
        Extract data from the data warehouse
        
        Returns:
            pd.DataFrame: The extracted data
        """
        try:
            ti = kwargs['ti']
            execution_date = ti.execution_date
            tz = pytz.timezone('Asia/Jakarta')
            execution_date = execution_date.astimezone(tz)

            # Get connection string from Airflow Variables
            # Connect to PostgreSQL database
            postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
            engine = create_engine(postgres_uri)
            
            # Load fact_order_items and related dimension tables
            df = pd.read_sql(query, engine)
            if df.empty:
                    ti.xcom_push(
                        key=f"extract_info-query", 
                        value={"status": "skipped", "data_date": execution_date}
                    )
                    raise AirflowSkipException(f"Query results doesn't have data. Skipped...")
            else:
                ti.xcom_push(
                    key=f"extract_info-query", 
                    value={"status": "success", "data_date": execution_date}
                )
                
                return df
            
        except AirflowSkipException as e:
            raise e
        
        except AirflowException as e:
            raise AirflowException(f"Error when extracting Query : {str(e)}")
        return df