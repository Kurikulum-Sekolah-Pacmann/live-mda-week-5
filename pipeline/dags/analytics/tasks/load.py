from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine
import os
import logging
import pandas as pd

class Load:
    @staticmethod
    def save_results(features, segment_analysis, **kwargs):
        """
        Save results to CSV first, then insert into the data warehouse using truncate then insert pattern.
        
        Args:
            features: DataFrame with user features and segments
            segment_analysis: DataFrame with segment analysis
        """
        try:
            logging.info("Starting save_results process...")
            ti = kwargs['ti']
            
            # Check if DataFrames are empty
            if features.empty:
                logging.warning("Features DataFrame is empty! Cannot proceed.")
                raise AirflowException("Features DataFrame is empty")
                
            if segment_analysis.empty:
                logging.warning("Segment analysis DataFrame is empty! Cannot proceed.")
                raise AirflowException("Segment analysis DataFrame is empty")
            
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
                user_segments_df = features[['user_id', 'segment']]
                # 2. Prepare search personalization rules
                search_rules = segment_analysis[['segment', 'avg_total_spent']].rename(columns={'avg_total_spent': 'priority'})
                search_rules['priority'] = search_rules['priority'].rank(ascending=False)

            except Exception as e:
                logging.error(f"Error preparing DataFrames: {str(e)}")
                raise AirflowException(f"Error preparing DataFrames: {str(e)}")
            
            # Begin transaction truncate
            try:
                # Truncate and insert dim_user_segments
                cursor.execute("TRUNCATE TABLE dim_user_segments;")
                logging.info("Truncated dim_user_segments successfully.")
                
                # Truncate and insert dim_segments
                cursor.execute("TRUNCATE TABLE dim_segments;")
                logging.info("Truncated dim_segments successfully.")

                # Truncate and insert search_personalization_rules
                cursor.execute("TRUNCATE TABLE search_personalization_rules;")
                logging.info("Truncated search_personalization_rules successfully.")

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
                # Insert user segments
                user_segments_df.to_sql('dim_user_segments', engine, if_exists='append', index=False)
                logging.info("Inserted user segments successfully.")
                
                # Insert segments
                segment_analysis.to_sql('dim_segments', engine, if_exists='append', index=False)
                logging.info("Inserted segments successfully.")
                
                # Insert search personalization rules
                search_rules.to_sql('search_personalization_rules', engine, if_exists='append', index=False)
                logging.info("Inserted search personalization rules successfully.")
            except Exception as e:
                logging.error(f"Error inserting data to warehouse: {str(e)}")
                raise AirflowException(f"Error inserting data to warehouse: {str(e)}")
            
            # Push information to XCom
            ti.xcom_push(
                key="load_data_info", 
                value={
                    "status": "success", 
                    "user_segments_count": len(user_segments_df),
                    "segments_count": len(segment_analysis),
                    "rules_count": len(search_rules)
                }
            )
            logging.info("Pushed data to XCom successfully.")
            
            return True
            
        except Exception as e:
            logging.error(f"Error saving results to warehouse: {str(e)}")
            raise AirflowException(f"Error saving results to warehouse: {str(e)}")