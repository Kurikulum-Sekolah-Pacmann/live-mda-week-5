import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from airflow.exceptions import AirflowException, AirflowSkipException


class Analytics:
    @staticmethod
    def create_features(df, **kwargs):
        """
        Create features for customer segmentation
        
        Args:
            df: DataFrame with raw e-commerce data
            
        Returns:
            pd.DataFrame: Features for each user
        """
        try:
            ti = kwargs['ti']
            
            # Purchase behavior metrics
            purchase_behavior = df.groupby('user_id').agg(
                total_orders=('order_nk', 'nunique'),
                total_spent=('order_price_usd', 'sum'),
                avg_order_value=('order_price_usd', 'mean')
            ).reset_index()

            # Category preferences
            category_spending = df.groupby(['user_id', 'product_name']).agg(
                category_spent=('item_price_usd', 'sum')
            ).reset_index()
            category_spending = category_spending.pivot_table(index='user_id', columns='product_name', 
                                                            values='category_spent', fill_value=0)

            # Price sensitivity
            price_sensitivity = df.groupby('user_id').agg(
                avg_item_price=('item_price_usd', 'mean')
            ).reset_index()

            # Browsing patterns 
            browsing_patterns = df.groupby('user_id').agg(
                total_items_purchased=('items_purchased', 'sum'),
                avg_time_since_first_order=(
                    'date_actual', 
                    lambda x: (pd.Timestamp.now() - pd.to_datetime(x.min())).days  # Ensure it's a Timestamp
                )
            ).reset_index()

            # Merge all features
            features = purchase_behavior.merge(category_spending, on='user_id', how='left') \
                                        .merge(price_sensitivity, on='user_id', how='left') \
                                        .merge(browsing_patterns, on='user_id', how='left')
            features = features.fillna(0)
            
            if features.empty:
                ti.xcom_push(
                    key="create_features_info", 
                    value={"status": "skipped"}
                )
                raise AirflowSkipException("No features created. Skipping...")
            else:
                ti.xcom_push(
                    key="create_features_info", 
                    value={"status": "success", "user_count": len(features)}
                )
                
                return features
                
        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error creating features: {str(e)}")
    
    @staticmethod
    def segment_users(features, n_clusters=5, **kwargs):
        """
        Segment users using K-means clustering
        
        Args:
            features: DataFrame with user features
            n_clusters: Number of clusters to create
            
        Returns:
            pd.DataFrame: Features with segment labels
        """
        try:
            ti = kwargs['ti']
            
            # Deep copy the features to avoid modifying the original
            features_copy = features.copy()
            
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features_copy.drop(columns=['user_id']))
            
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            features_copy['segment'] = kmeans.fit_predict(scaled_features)
            
            ti.xcom_push(
                key="segment_users_info", 
                value={"status": "success", "cluster_count": n_clusters}
            )
            
            return features_copy
            
        except Exception as e:
            raise AirflowException(f"Error segmenting users: {str(e)}")
    
    @staticmethod
    def analyze_segments(features, **kwargs):
        """
        Analyze user segments
        
        Args:
            features: DataFrame with user features and segments
            
        Returns:
            pd.DataFrame: Segment analysis
        """
        try:
            ti = kwargs['ti']
            
            segment_analysis = features.groupby('segment').agg(
                user_count=('user_id', 'count'),
                avg_total_orders=('total_orders', 'mean'),
                avg_total_spent=('total_spent', 'mean'),
                avg_avg_order_value=('avg_order_value', 'mean'),
                avg_avg_item_price=('avg_item_price', 'mean'),
                avg_total_items_purchased=('total_items_purchased', 'mean'),
                avg_time_since_first_order=('avg_time_since_first_order', 'mean')
            ).reset_index()
            
            ti.xcom_push(
                key="analyze_segments_info", 
                value={"status": "success", "segment_count": len(segment_analysis)}
            )
            
            return segment_analysis
            
        except Exception as e:
            raise AirflowException(f"Error analyzing segments: {str(e)}")