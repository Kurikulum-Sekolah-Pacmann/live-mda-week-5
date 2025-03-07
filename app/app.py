import streamlit as st
import typesense
import os

# Set up Typesense client
def setup_typesense_client() -> typesense.Client:
    """
    Set up and return a Typesense client using Airflow connection.
    """
    
    client = typesense.Client({
        "api_key": os.getenv("TYPESENSE_API_KEY", "your_api_key"),
        "nodes": [{
            "host": os.getenv("TYPESENSE_HOST", "http://typesense:8108").replace("http://", "").split(":")[0],
            "port": os.getenv("TYPESENSE_HOST", "http://typesense:8108").split(":")[-1],
            "protocol": "http"
        }],
        "connection_timeout_seconds": 5
    })
    
    return client

# Search function for users
def search_users(query: str, client: typesense.Client) -> list:
    """
    Search for users in the Typesense collection.
    """
    search_parameters = {
        "q": query,
        "query_by": "user_id",
        "per_page": 10
    }
    return client.collections["users"].documents.search(search_parameters)

# Search function for products
def search_products(query: str, client: typesense.Client) -> list:
    """
    Search for products in the Typesense collection.
    """
    search_parameters = {
        "q": query,
        "query_by": "name",
        "per_page": 10
    }
    return client.collections["products"].documents.search(search_parameters)

def display_all_users(client: typesense.Client):
    """
    Display all users in the 'users' collection.
    """
    st.subheader("All Users in Collection")
    
    # Retrieve all users
    search_parameters = {
        "q": "*", 
        "query_by": "segment",  
        "per_page": 1  
    }
    results = client.collections["users"].documents.search(search_parameters)
    
    if results["found"] > 0:
        for hit in results["hits"]:
            st.write(f"**User ID:** {hit['document']['id']}")
            st.write(f"**Segment:** {hit['document']['segment']}")
            st.write(f"**Recommended Products:** {', '.join(hit['document']['recommended_products'])}")
            st.write("---")
    else:
        st.write("No users found in the collection.")

# Streamlit App
def main():
    st.title("Typesense Search Engine")
    
    # Initialize Typesense client
    client = setup_typesense_client()
    
    # Sidebar for search options
    st.sidebar.header("Search Options")
    search_type = st.sidebar.radio("Search Type", ["Users", "Products", "View All Users"])
    
    # Search bar (only for search functionality)
    if search_type in ["Users", "Products"]:
        query = st.text_input("Enter your search query:")
    
    if search_type == "Users":
        if query:
            st.subheader("Search Results for Users")
            results = search_users(query, client)
            if results["found"] > 0:
                for hit in results["hits"]:
                    st.write(f"**User ID:** {hit['document']['id']}")
                    st.write(f"**Segment:** {hit['document']['segment']}")
                    st.write(f"**Recommended Products:** {', '.join(hit['document']['recommended_products'])}")
                    st.write("---")
            else:
                st.write("No users found.")
    
    elif search_type == "Products":
        if query:
            st.subheader("Search Results for Products")
            results = search_products(query, client)
            if results["found"] > 0:
                for hit in results["hits"]:
                    st.write(f"**Product ID:** {hit['document']['id']}")
                    st.write(f"**Product Name:** {hit['document']['name']}")
                    st.write(f"**Created At:** {hit['document']['created_at']}")
                    st.write("---")
            else:
                st.write("No products found.")
    
    elif search_type == "View All Users":
        display_all_users(client)

# Run the Streamlit app
if __name__ == "__main__":
    main()