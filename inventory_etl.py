# inventory_etl.py
# This script fetches the latest inventory snapshot from Odoo and uploads it to BigQuery.

import os
import requests
import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- SECURE METHOD: Load settings from environment variables ---
# These are provided by GitHub Actions secrets.
ODOO_URL = os.environ.get("ODOO_URL")
ODOO_DB = os.environ.get("ODOO_DB")
ODOO_USERNAME = os.environ.get("ODOO_USERNAME")
ODOO_PASSWORD = os.environ.get("ODOO_PASSWORD")

# --- BigQuery Settings ---
PROJECT_ID = "spartan-cedar-467808-p9"
DATASET_ID = "Orders"
STOCK_TABLE = "stock_data"
# --- MODIFICATION: CREDENTIALS_FILE_PATH is no longer needed. ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_odoo_session(url, db, username, password):
    """Establishes a session with the Odoo server."""
    # Check if credentials were loaded correctly
    if not all([url, db, username, password]):
        logging.error("❌ Odoo environment variables not set correctly.")
        return None

    auth_url = f"{url}/web/session/authenticate"
    payload = {"jsonrpc": "2.0", "params": {"db": db, "login": username, "password": password}}
    
    try:
        session = requests.Session()
        response = session.post(auth_url, json=payload, timeout=30)
        response.raise_for_status()
        if response.json().get("result", {}).get("uid"):
            logging.info("✅ Odoo authentication successful.")
            return session
        else:
            logging.error(f"❌ Odoo authentication failed: {response.json().get('error', 'No error message.')}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Authentication request failed: {e}")
        return None

def call_odoo_rpc(session, url, model, method, params):
    """Makes a remote procedure call to the Odoo server."""
    rpc_url = f"{url}/web/dataset/call_kw"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            'model': model,
            'method': method,
            'args': params.get('args', []),
            'kwargs': params.get('kwargs', {})
        }
    }
    try:
        response = session.post(rpc_url, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()
        if result.get('error'):
            error_details = result['error']
            logging.error(f"❌ Odoo RPC Error: {error_details.get('message')}\n{error_details.get('data', {}).get('debug', '')}")
            return None
        return result.get('result')
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ RPC call failed: {e}")
        return None

def ensure_stock_table_exists(client, dataset_id, table_id):
    """Checks if the BigQuery table exists and creates it if it doesn't."""
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        logging.info(f"Table {table_id} not found, creating it...")
        # --- MODIFICATION: Use BQ-friendly column names (no spaces) ---
        schema = [
            bigquery.SchemaField("Product_Name", "STRING"),
            bigquery.SchemaField("Barcode", "STRING"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("Qty_On_Hand", "FLOAT"),
            bigquery.SchemaField("Reserved_Qty", "FLOAT"),
            bigquery.SchemaField("Available_Qty", "FLOAT"),
            bigquery.SchemaField("Unit_Cost", "FLOAT"),
            bigquery.SchemaField("Total_Cost", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Created table {table_id} in dataset {dataset_id}.")

def main():
    """Main ETL pipeline function."""
    logging.info("Connecting to Odoo...")
    session = get_odoo_session(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    if not session:
        # Exit with a non-zero status code to fail the GitHub Action
        exit(1)

    logging.info("📦 Fetching product data...")
    products = call_odoo_rpc(session, ODOO_URL, "product.product", "search_read", {
        "args": [[("type", "=", "product")]],
        "kwargs": {"fields": ["id", "display_name", "barcode", "standard_price", "categ_id"]}
    })
    if not products:
        logging.error("No products found or an error occurred. Exiting.")
        exit(1)

    products_df = pd.DataFrame(products)
    products_df["product_id"] = products_df["id"]
    products_df["category"] = products_df["categ_id"].apply(lambda x: x[1] if isinstance(x, list) else "")
    logging.info(f"✅ Loaded {len(products_df)} products.")

    logging.info("📊 Fetching stock quantities...")
    stock_quants = call_odoo_rpc(session, ODOO_URL, "stock.quant", "search_read", {
        "args": [[("location_id.usage", "=", "internal")]],
        "kwargs": {"fields": ["product_id", "quantity", "reserved_quantity"]}
    })
    if stock_quants is None:
        logging.error("Could not fetch stock quantities. Exiting.")
        exit(1)

    if not stock_quants:
        stock_summary = pd.DataFrame(columns=['product_id', 'on_hand_quantity', 'reserved_quantity'])
    else:
        quant_df = pd.DataFrame(stock_quants)
        quant_df["product_id"] = quant_df["product_id"].apply(lambda x: x[0] if isinstance(x, list) else x)
        stock_summary = quant_df.groupby("product_id").agg(
            on_hand_quantity=("quantity", "sum"),
            reserved_quantity=("reserved_quantity", "sum")
        ).reset_index()

    logging.info("📎 Merging data...")
    df = pd.merge(products_df, stock_summary, on="product_id", how="left")

    # --- MODIFICATION: Modern pandas method to fill NA and avoid FutureWarning ---
    # This is the correct way to fill missing values.
    for col in ["on_hand_quantity", "reserved_quantity", "standard_price"]:
        df[col] = df[col].fillna(0)
    
    df["available_quantity"] = df["on_hand_quantity"] - df["reserved_quantity"]
    df["total_cost"] = df["on_hand_quantity"] * df["standard_price"]

    # --- MODIFICATION: Use BQ-friendly column names ---
    final_df = df[["display_name", "barcode", "category", "on_hand_quantity", "reserved_quantity", "available_quantity", "standard_price", "total_cost"]]
    final_df.columns = ["Product_Name", "Barcode", "Category", "Qty_On_Hand", "Reserved_Qty", "Available_Qty", "Unit_Cost", "Total_Cost"]

    logging.info(f"📤 Uploading to BigQuery table: {DATASET_ID}.{STOCK_TABLE}")
    try:
        # --- MODIFICATION: Automated authentication ---
        # The client will automatically find credentials from the environment.
        # No need to load a JSON file manually.
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        ensure_stock_table_exists(bq_client, DATASET_ID, STOCK_TABLE)
        
        # `to_gbq` will also find credentials automatically from the environment.
        final_df.to_gbq(
            destination_table=f"{DATASET_ID}.{STOCK_TABLE}",
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        logging.info(f"✅ Data uploaded to BigQuery table: {DATASET_ID}.{STOCK_TABLE}")
    except Exception as e:
        logging.error(f"❌ Failed to upload to BigQuery: {e}")
        # Raise the exception to make the GitHub Action fail
        raise

if __name__ == "__main__":
    main()