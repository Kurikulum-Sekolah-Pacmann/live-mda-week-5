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

# Search function
def search_collection(collection_name: str, query: str, search_fields: list) -> list:
    """
    Search a Typesense collection and return results.
    """
    client = setup_typesense_client()
    
    search_parameters = {
        "q": query,
        "query_by": ",".join(search_fields),
        "per_page": 10  # Limit results to 10 items
    }
    
    try:
        results = client.collections[collection_name].documents.search(search_parameters)
        return results["hits"]
    except Exception as e:
        st.error(f"Error searching {collection_name}: {str(e)}")
        return []

# Streamlit app
def main():
    st.title("Typesense Search Engine")
    
    # Sidebar for search options
    st.sidebar.header("Search Options")
    collection_name = st.sidebar.selectbox("Select Collection", ["users", "products"])
    query = st.sidebar.text_input("Enter Search Query")
    
    # Define search fields based on collection
    if collection_name == "users":
        search_fields = ["segment", "avg_order_value", "total_spent"]  # Removed "id"
    elif collection_name == "products":
        search_fields = ["name", "created_at"]  # Removed "id"
    
    # Perform search
    if st.sidebar.button("Search"):
        if query:
            st.write(f"Searching {collection_name} for: `{query}`")
            
            # Get search results
            results = search_collection(collection_name, query, search_fields)
            
            if results:
                st.write(f"Found {len(results)} results:")
                
                # Display results in a table
                if collection_name == "users":
                    st.table([{
                        "ID": hit["document"]["id"],  # Display ID but don't search by it
                        "Segment": hit["document"]["segment"],
                        "Avg Order Value": hit["document"]["avg_order_value"],
                        "Total Spent": hit["document"]["total_spent"]
                    } for hit in results])
                elif collection_name == "products":
                    st.table([{
                        "ID": hit["document"]["id"],  # Display ID but don't search by it
                        "Name": hit["document"]["name"],
                        "Created At": hit["document"]["created_at"]
                    } for hit in results])
            else:
                st.warning("No results found.")
        else:
            st.warning("Please enter a search query.")

# Run the app
if __name__ == "__main__":
    main()