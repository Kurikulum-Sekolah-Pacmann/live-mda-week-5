from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine
import os
import logging
import pandas as pd

class Load:
    @staticmethod
    def save_results(features, **kwargs):
        """
        Save results to the data warehouse using truncate then insert pattern.
        
        Args:
            features: DataFrame with user features, segments and product preferences
        """
        try:
            logging.info("Starting save_results process...")
            ti = kwargs['ti']
            
            # Check if DataFrame is empty
            if features.empty:
                logging.warning("Features DataFrame is empty! Cannot proceed.")
                raise AirflowException("Features DataFrame is empty")
            
            # Get connection from PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id='warehouse')
            postgres_uri = postgres_hook.get_uri()
            
            engine = create_engine(postgres_uri)
            
            # Get the database connection
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            # Prepare the data
            try:
                # 1. Prepare user segments
                user_segments_df = features[['user_id', 'segment', 'product_preferences']]
                user_segments_df = user_segments_df.rename(columns={'product_preferences': 'recommended_products'})
                
                # Convert product_preferences list to PostgreSQL array format
                def format_product_array(product_list):
                    # Convert product IDs to integers and format as PostgreSQL array
                    if not product_list:
                        return '{}'
                    return '{' + ','.join(str(int(float(p))) for p in product_list) + '}'
                
                user_segments_df['recommended_products'] = user_segments_df['recommended_products'].apply(format_product_array)
                
                # 2. Prepare dim_segments data
                # Get unique segments with descriptions
                segments_df = pd.DataFrame({
                    'segment_name': features['segment'].unique()
                })
                
                # Generate descriptions based on segment names
                segments_df['description'] = segments_df['segment_name'].apply(
                    lambda name: f"Customer segment characterized as {name.lower()} based on purchase behavior analysis."
                )

            except Exception as e:
                logging.error(f"Error preparing DataFrames: {str(e)}")
                raise AirflowException(f"Error preparing DataFrames: {str(e)}")
            
            # Begin transaction truncate
            try:
                # Truncate and insert dim_user_segments
                cursor.execute("TRUNCATE TABLE dim_user_segments CASCADE;")
                logging.info("Truncated dim_user_segments successfully.")
                
                # Truncate and insert dim_segments
                cursor.execute("TRUNCATE TABLE dim_segments CASCADE;")
                logging.info("Truncated dim_segments successfully.")

                # Commit the transaction
                conn.commit()
                logging.info("Transaction committed successfully.")
            except Exception as e:
                conn.rollback()
                logging.error(f"Error in database operations: {str(e)}")
                raise AirflowException(f"Error in database operations: {str(e)}")
            finally:
                # Close cursor and connection
                cursor.close()
                conn.close()
                logging.info("Database connection closed.")

            # Insert data
            try:
                 # Insert segments
                segments_df.to_sql('dim_segments', engine, if_exists='append', index=False)
                logging.info("Inserted segments successfully.")

                # Insert user segments
                user_segments_df.to_sql('dim_user_segments', engine, if_exists='append', index=False)
                logging.info("Inserted user segments successfully.")

            except Exception as e:
                logging.error(f"Error inserting data to warehouse: {str(e)}")
                raise AirflowException(f"Error inserting data to warehouse: {str(e)}")
            
            # Push information to XCom
            ti.xcom_push(
                key="load_data_info", 
                value={
                    "status": "success", 
                    "user_segments_count": len(user_segments_df),
                    "segments_count": len(segments_df)
                }
            )
            logging.info("Pushed data to XCom successfully.")
            
            return True
            
        except Exception as e:
            logging.error(f"Error saving results to warehouse: {str(e)}")
            raise AirflowException(f"Error saving results to warehouse: {str(e)}")