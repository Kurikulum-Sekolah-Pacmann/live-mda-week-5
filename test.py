import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine

# Load data from the data warehouse
def extract_data(connection_string):
    engine = create_engine(connection_string)
    
    # Load fact_order_items and related dimension tables
    query = """
    SELECT 
        foi.user_id,
        foi.order_id,
        foi.product_id,
        foi.is_primary_item,
        foi.item_price_usd,
        foi.item_cogs_usd,
        foi.order_created_at,
        foi.items_purchased,
        foi.order_price_usd,
        foi.order_cogs_usd,
        dp.product_name,
        du.created_at AS user_created_at
    FROM fact_order_items foi
    JOIN dim_products dp ON foi.product_id = dp.product_id
    JOIN dim_users du ON foi.user_id = du.user_id
    """
    df = pd.read_sql(query, engine)
    return df

# Feature creation
def create_features(df):
    # Purchase behavior metrics
    purchase_behavior = df.groupby('user_id').agg(
        total_orders=('order_id', 'nunique'),
        total_spent=('order_price_usd', 'sum'),
        avg_order_value=('order_price_usd', 'mean')
    ).reset_index()

    # Category preferences
    category_spending = df.groupby(['user_id', 'product_name']).agg(
        category_spent=('item_price_usd', 'sum')
    ).reset_index()
    category_spending = category_spending.pivot_table(index='user_id', columns='product_name', values='category_spent', fill_value=0)

    # Price sensitivity
    price_sensitivity = df.groupby('user_id').agg(
        avg_item_price=('item_price_usd', 'mean')
    ).reset_index()

    # Browsing patterns (assuming browsing data is in fact_order_items)
    browsing_patterns = df.groupby('user_id').agg(
        total_items_purchased=('items_purchased', 'sum'),
        avg_time_since_first_order=('order_created_at', lambda x: (pd.Timestamp.now() - x.min()).days)
    ).reset_index()

    # Merge all features
    features = purchase_behavior.merge(category_spending, on='user_id', how='left') \
                                .merge(price_sensitivity, on='user_id', how='left') \
                                .merge(browsing_patterns, on='user_id', how='left')
    features = features.fillna(0)
    return features

# User segmentation using K-means clustering
def segment_users(features, n_clusters=5):
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features.drop(columns=['user_id']))
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    features['segment'] = kmeans.fit_predict(scaled_features)
    return features

# Segment analysis
def analyze_segments(features):
    segment_analysis = features.groupby('segment').agg(
        avg_total_orders=('total_orders', 'mean'),
        avg_total_spent=('total_spent', 'mean'),
        avg_avg_order_value=('avg_order_value', 'mean'),
        avg_avg_item_price=('avg_item_price', 'mean'),
        avg_total_items_purchased=('total_items_purchased', 'mean'),
        avg_time_since_first_order=('avg_time_since_first_order', 'mean')
    ).reset_index()
    return segment_analysis

# Save results to the data warehouse
def save_results(features, segment_analysis, connection_string):
    engine = create_engine(connection_string)

    # Save user segments
    features[['user_id', 'segment']].to_sql('dim_user_segments', engine, if_exists='replace', index=False)

    # Save segment definitions
    segment_analysis.to_sql('dim_segments', engine, if_exists='replace', index=False)

    # Save search personalization rules (example: prioritize high-spending segments)
    search_rules = segment_analysis[['segment', 'avg_total_spent']].rename(columns={'avg_total_spent': 'priority'})
    search_rules['priority'] = search_rules['priority'].rank(ascending=False)
    search_rules.to_sql('search_personalization_rules', engine, if_exists='replace', index=False)

# Main function
def main():
    # Database connection string
    connection_string = "postgresql://user:password@host:port/database"

    # Load data
    df = extract_data(connection_string)

    # Create features
    features = create_features(df)

    # Segment users
    features = segment_users(features)

    # Analyze segments
    segment_analysis = analyze_segments(features)

    # Save results
    save_results(features, segment_analysis, connection_string)

if __name__ == "__main__":
    main()