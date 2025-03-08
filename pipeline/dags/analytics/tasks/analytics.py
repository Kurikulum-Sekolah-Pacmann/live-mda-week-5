import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Analytics:
    @staticmethod
    def create_features(df, **kwargs):
        """
        Create features for customer segmentation
        
        Args:
            df: DataFrame with raw data
            
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
            category_spending = df.groupby(['user_id', 'product_id']).agg(
                category_spent=('item_price_usd', 'sum')
            ).reset_index()
            category_spending = category_spending.pivot_table(index='user_id', columns='product_id', 
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
        Segment users using K-means clustering and assign segment names.
        """
        try:
            ti = kwargs['ti']
            
            # Deep copy the features to avoid modifying the original
            features_copy = features.copy()
            
            # Get feature columns (exclude user_id)
            feature_columns = [col for col in features_copy.columns if col != 'user_id']
            
            # Scale the features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features_copy[feature_columns])
            
            # Apply KMeans clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = kmeans.fit_predict(scaled_features)
            
            # Add numeric segment to features
            features_copy['cluster_id'] = cluster_labels
            
            
            ti.xcom_push(
                key="segment_users_info", 
                value={
                    "status": "success", 
                    "cluster_count": n_clusters
                }
            )
            
            return features_copy
                
        except Exception as e:
            raise AirflowException(f"Error segmenting users: {str(e)}")
    
    @staticmethod
    def analyze_segments(features, **kwargs):
        """
        Analyze user segments and prepare dim_segments and user_segments data.
        
        Args:
            features: DataFrame with user features and segments
            
        Returns:
            dict: Contains segment_analysis, dim_segments, and user_segments DataFrames
        """
        try:
            ti = kwargs['ti']
            
            # Create segment names
            segment_descriptions = {}
            cluster_means = features.groupby('cluster_id').agg({
                'total_spent': 'mean',
                'avg_order_value': 'mean',
                'avg_item_price': 'mean',
                'total_orders': 'mean',
                'total_items_purchased': 'mean',
                'avg_time_since_first_order': 'mean'
            })
            
            for cluster_id, row in cluster_means.iterrows():
                if row['total_spent'] >= cluster_means['total_spent'].quantile(0.75):
                    spending = "High Spender"
                elif row['total_spent'] <= cluster_means['total_spent'].quantile(0.25):
                    spending = "Budget Conscious" 
                else:
                    spending = "Moderate Spender"
                
                if row['total_orders'] >= cluster_means['total_orders'].quantile(0.75):
                    frequency = "Frequent"
                elif row['total_orders'] <= cluster_means['total_orders'].quantile(0.25):
                    frequency = "Occasional"
                else:
                    frequency = "Regular"
                
                if row['avg_time_since_first_order'] <= cluster_means['avg_time_since_first_order'].quantile(0.25):
                    recency = "New"
                elif row['avg_time_since_first_order'] >= cluster_means['avg_time_since_first_order'].quantile(0.75):
                    recency = "Loyal"
                else:
                    recency = ""
                
                components = [comp for comp in [recency, frequency, spending] if comp]
                segment_name = " ".join(components)
                segment_descriptions[cluster_id] = segment_name
            
            # Map the descriptive names to the features dataframe
            features['segment'] = features['cluster_id'].map(segment_descriptions)
            
            # Get top product recommendations for each user based on product_id
            product_columns = [col for col in features.columns 
                            if col not in ['user_id', 'cluster_id', 'segment', 'total_orders', 
                                            'total_spent', 'avg_order_value', 'avg_item_price',
                                            'total_items_purchased', 'avg_time_since_first_order']]
            
            # For each user, get their top 3 product_ids
            features['product_preferences'] = features.apply(
                lambda row: [col for col, val in sorted(
                    [(col, row[col]) for col in product_columns],
                    key=lambda x: x[1], reverse=True
                )[:3] if val > 0],
                axis=1
            )
            
            
            
            # Push information to XCom
            ti.xcom_push(
                key="analyze_segments_info", 
                value={
                    "status": "success"
                }
            )
            
            # Return all DataFrames
            return features
                
        except Exception as e:
            raise AirflowException(f"Error analyzing segments: {str(e)}")