import typesense
from airflow.hooks.base import BaseHook
from typing import List, Dict, Any
from airflow.exceptions import AirflowSkipException, AirflowException

class Load:
    @staticmethod
    def setup_typesense_client() -> typesense.Client:
        """
        Set up and return a Typesense client using Airflow connection.
        """
        conn = BaseHook.get_connection("typesense")
        
        client = typesense.Client({
            "api_key": conn.extra_dejson["api_key"],
            "nodes": [{
                "host": conn.host,
                "port": conn.port,
                "protocol": conn.schema
            }],
            "connection_timeout_seconds": 5
        })
        
        return client
    
    @staticmethod
    def remove_collection_if_exists(client: typesense.Client, collection_name: str) -> None:
        """
        Remove a collection in Typesense if it exists.
        """
        try:
            client.collections[collection_name].delete()
        except typesense.exceptions.ObjectNotFound:
            pass

    @staticmethod
    def create_collection_if_not_exists(client: typesense.Client, collection_name: str, schema: Dict) -> None:
        """
        Create a collection in Typesense if it does not exist.
        """
        try:
            client.collections[collection_name].retrieve()
        except typesense.exceptions.ObjectNotFound:
            client.collections.create(schema)

    @staticmethod
    def load_users_to_typesense(user_data: List[Dict[str, Any]], **context) -> None:
        """
        Load user data to Typesense
        """
        try:
            client = Load.setup_typesense_client()

            users_schema = {
                "name": "users",
                "fields": [
                    {"name": "id", "type": "string"},  # user_id
                    {"name": "user_id", "type": "string"},  # user_id
                    {"name": "segment", "type": "string"},
                    {"name": "recommended_products", "type": "string[]"}  # list of product_ids
                ]
            }

            # Create collection if not exists
            Load.create_collection_if_not_exists(client, "users", users_schema)

            # Import data in batch
            batch_size = 100
            for i in range(0, len(user_data), batch_size):
                batch = user_data[i:i + batch_size]
                try:
                    client.collections["users"].documents.import_(batch, {"action": "upsert"})
                except Exception as e:
                    raise AirflowException(f"Error : {str(e)}")
            
            # Load.remove_collection_if_exists(client, "users")
        except Exception as e:
            raise AirflowException(f"Error : {str(e)}")

    @staticmethod
    def load_products_to_typesense(product_data: List[Dict[str, Any]], **context) -> None:
        """
        Load product data to Typesense
        """
        client = Load.setup_typesense_client()

        products_schema = {
            "name": "products",
            "fields": [
                {"name": "id", "type": "string"},  # product_id
                {"name": "name", "type": "string"},  # product_name
                {"name": "created_at", "type": "string"}  # created_at (nullable)
            ]
        }

        # Create collection if not exists
        Load.create_collection_if_not_exists(client, "products", products_schema)

        # Import data in batch
        batch_size = 100
        for i in range(0, len(product_data), batch_size):
            batch = product_data[i:i + batch_size]
            try:
                client.collections["products"].documents.import_(batch, {"action": "upsert"})
            except Exception as e:
                print(f"Error importing products batch: {str(e)}")

