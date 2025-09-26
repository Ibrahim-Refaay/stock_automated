# inventory_etl_github.py
# This script fetches the latest inventory snapshot from Odoo and uploads it to BigQuery.
# Optimized for GitHub Actions with environment variables and automatic authentication.

import os
import requests
import pandas as pd
import logging
from datetime import datetime
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

# --- File Configuration ---
# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(SCRIPT_DIR, "locations-id.xlsx")
BRANCHES_FILE = os.path.join(SCRIPT_DIR, "Branches-id.xlsx")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def get_odoo_session():
    """Authenticates with Odoo and returns a session object and user ID."""
    # Check if credentials were loaded correctly
    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD]):
        logging.error("❌ Odoo environment variables not set correctly.")
        logging.error("Required: ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD")
        return None, None

    session = requests.Session()
    auth_url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0", 
        "method": "call", 
        "params": {
            "db": ODOO_DB, 
            "login": ODOO_USERNAME, 
            "password": ODOO_PASSWORD
        }
    }
    
    try:
        logging.info("Authenticating to Odoo...")
        response = session.post(auth_url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        uid = result.get('result', {}).get('uid')
        
        if uid:
            logging.info(f"✅ Odoo authentication successful. UID: {uid}")
            return session, uid
        else:
            logging.error(f"❌ Odoo login failed: {result.get('error', 'Unknown error')}")
            return None, None
    except Exception as e:
        logging.error(f"❌ Odoo connection error: {e}")
        return None, None
    """Load Excel files for locations and branches mapping."""
    try:
        # Try to read Excel files
        if os.path.exists(LOCATIONS_FILE) and os.path.exists(BRANCHES_FILE):
            logging.info(f"📁 Reading Excel files from: {SCRIPT_DIR}")
            locations_df = pd.read_excel(LOCATIONS_FILE)
            branches_df = pd.read_excel(BRANCHES_FILE)
            
            target_location_ids = locations_df['id'].tolist()
            branch_map = pd.Series(branches_df.name.values, index=branches_df.id).to_dict()
            
            logging.info(f"✅ Loaded {len(target_location_ids)} locations and {len(branch_map)} branches from Excel files.")
            return target_location_ids, branch_map, True
        else:
            logging.warning(f"⚠️ Excel files not found at:")
            logging.warning(f"   - {LOCATIONS_FILE}")
            logging.warning(f"   - {BRANCHES_FILE}")
            logging.info("📋 Will fetch location and branch data from Odoo directly.")
            return None, None, False
            
    except Exception as e:
        logging.error(f"❌ Error reading Excel files: {e}")
        logging.info("📋 Will fetch location and branch data from Odoo directly.")
        return None, None, False

def get_locations_and_branches_from_odoo(session, uid):
    """Fetch locations and branches directly from Odoo if Excel files are not available."""
    try:
        # Get internal locations
        logging.info("📍 Fetching internal stock locations from Odoo...")
        locations = call_odoo(session, uid, "stock.location", "search_read", 
                            [[('usage', '=', 'internal')]], 
                            {"fields": ["id", "name", "warehouse_id"]})
        
        if not locations:
            logging.error("❌ No internal locations found in Odoo.")
            return [], {}
            
        location_ids = [loc['id'] for loc in locations]
        
        # Get warehouses (branches)
        logging.info("🏢 Fetching warehouse/branch information from Odoo...")
        warehouses = call_odoo(session, uid, "stock.warehouse", "search_read", 
                              [], {"fields": ["id", "name"]})
        
        branch_map = {w['id']: w['name'] for w in warehouses} if warehouses else {}
        
        logging.info(f"✅ Found {len(location_ids)} locations and {len(branch_map)} branches from Odoo.")
        return location_ids, branch_map
        
    except Exception as e:
        logging.error(f"❌ Error fetching locations/branches from Odoo: {e}")
        return [], {}
    """Authenticates with Odoo and returns a session object and user ID."""
    # Check if credentials were loaded correctly
    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD]):
        logging.error("❌ Odoo environment variables not set correctly.")
        logging.error("Required: ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD")
        return None, None

    session = requests.Session()
    auth_url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0", 
        "method": "call", 
        "params": {
            "db": ODOO_DB, 
            "login": ODOO_USERNAME, 
            "password": ODOO_PASSWORD
        }
    }
    
    try:
        logging.info("Authenticating to Odoo...")
        response = session.post(auth_url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        uid = result.get('result', {}).get('uid')
        
        if uid:
            logging.info(f"✅ Odoo authentication successful. UID: {uid}")
            return session, uid
        else:
            logging.error(f"❌ Odoo login failed: {result.get('error', 'Unknown error')}")
            return None, None
    except Exception as e:
        logging.error(f"❌ Odoo connection error: {e}")
        return None, None

def call_odoo(session, uid, model, method, args=[], kwargs={}, timeout=600):
    """Makes a JSON-RPC call to Odoo with proper error handling."""
    rpc_url = f"{ODOO_URL}/jsonrpc"
    payload = {
        "jsonrpc": "2.0", 
        "method": "call", 
        "params": {
            "service": "object", 
            "method": "execute_kw", 
            "args": [ODOO_DB, uid, ODOO_PASSWORD, model, method, args, kwargs]
        }
    }
    
    try:
        response = session.post(rpc_url, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()
        
        if 'error' in result:
            logging.error(f"❌ RPC Error on {model}.{method}: {result['error']['data']['message']}")
            return []
        return result.get('result', [])
    except Exception as e:
        logging.error(f"❌ RPC call to {model}.{method} failed: {e}")
        return []

def fetch_in_batches(session, uid, model, ids, fields):
    """Fetches records in batches to avoid hitting URL length limits."""
    batch_size = 1000
    all_records = []
    
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        logging.info(f"  -> Fetching batch {i//batch_size + 1}/{(len(ids) + batch_size - 1) // batch_size} for {model}...")
        records = call_odoo(session, uid, model, "read", [batch_ids], {"fields": fields})
        if records:
            all_records.extend(records)
            
    return all_records

def ensure_stock_table_exists(client, dataset_id, table_id):
    """Checks if the BigQuery table exists and creates it if it doesn't."""
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
        logging.info(f"✅ Table {table_id} already exists.")
    except NotFound:
        logging.info(f"🔧 Table {table_id} not found, creating it...")
        # --- MODIFICATION: Use BQ-friendly column names (no spaces) ---
        schema = [
            bigquery.SchemaField("Product_Name", "STRING"),
            bigquery.SchemaField("Barcode", "STRING"),
            bigquery.SchemaField("Branch_Name", "STRING"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("Qty_On_Hand", "FLOAT"),
            bigquery.SchemaField("Reserved_Qty", "FLOAT"),
            bigquery.SchemaField("Available_Qty", "FLOAT"),
            bigquery.SchemaField("Unit_Cost", "FLOAT"),
            bigquery.SchemaField("Total_Cost", "FLOAT"),
            bigquery.SchemaField("Last_Updated", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"✅ Created table {table_id} in dataset {dataset_id}.")

def main():
    """Main ETL pipeline function."""
    logging.info("=== Starting Comprehensive Stock Report for GitHub Actions ===")
    
    # --- 1. Authenticate with Odoo ---
    session, uid = get_odoo_session()
    if not session:
        logging.error("❌ Cannot proceed without Odoo authentication.")
        exit(1)

    logging.warning("⏳ This is a large data operation and may take several minutes to complete.")

    try:
        # --- 2. Get branch mapping ---
        logging.info("📋 Fetching branch information...")
        branches = call_odoo(session, uid, "stock.warehouse", "search_read", [], {"fields": ["id", "name"]})
        branch_map = {b['id']: b['name'] for b in branches} if branches else {}
        logging.info(f"✅ Found {len(branch_map)} branches.")

        # --- 3. Get location IDs for internal locations ---
        logging.info("📍 Fetching internal stock locations...")
        locations = call_odoo(session, uid, "stock.location", "search_read", 
                            [[('usage', '=', 'internal')]], 
                            {"fields": ["id", "name", "warehouse_id"]})
        
        if not locations:
            logging.error("❌ No internal locations found.")
            exit(1)
            
        location_ids = [loc['id'] for loc in locations]
        location_map = {loc['id']: {
            'name': loc['name'], 
            'warehouse_id': loc['warehouse_id'][0] if isinstance(loc.get('warehouse_id'), list) else None
        } for loc in locations}
        
        logging.info(f"✅ Found {len(location_ids)} internal locations.")

        # --- 4. Fetch Stock Quants ---
        logging.info("📦 Fetching current stock levels for all internal locations...")
        quant_domain = [('location_id', 'in', location_ids)]
        quant_fields = ['product_id', 'location_id', 'quantity', 'reserved_quantity']
        
        stock_quants = call_odoo(session, uid, "stock.quant", "search_read", 
                                [quant_domain], 
                                {"fields": quant_fields, "limit": 500000})

        if not stock_quants:
            logging.warning("⚠️ No stock found in any of the specified locations.")
            # Create empty DataFrame for consistency
            final_df = pd.DataFrame(columns=["Product_Name", "Barcode", "Branch_Name", "Category", 
                                           "Qty_On_Hand", "Reserved_Qty", "Available_Qty", 
                                           "Unit_Cost", "Total_Cost", "Last_Updated"])
        else:
            logging.info(f"✅ Found {len(stock_quants)} total stock records across all locations.")
            
            # --- 5. Get unique product IDs and fetch product details ---
            product_ids = list(set([q['product_id'][0] for q in stock_quants if q.get('product_id')]))
            logging.info(f"📝 Fetching details for {len(product_ids)} unique products...")
            
            product_fields = ['id', 'display_name', 'barcode', 'categ_id', 'standard_price']
            product_data = fetch_in_batches(session, uid, "product.product", product_ids, product_fields)
            product_map = {p['id']: p for p in product_data}

            # --- 6. Assemble the Final Report ---
            logging.info("⚙️ Assembling the final report...")
            report_data = []
            current_timestamp = datetime.utcnow()
            
            for quant in stock_quants:
                if not quant.get('product_id'):
                    continue
                    
                product_id = quant['product_id'][0]
                product_info = product_map.get(product_id, {})
                
                # Get location info
                location_id = quant['location_id'][0] if isinstance(quant.get('location_id'), list) else quant.get('location_id')
                location_info = location_map.get(location_id, {})
                warehouse_id = location_info.get('warehouse_id')
                branch_name = branch_map.get(warehouse_id, 'N/A')

                # Calculate quantities
                on_hand_qty = quant.get('quantity', 0)
                reserved_qty = quant.get('reserved_quantity', 0)
                available_qty = on_hand_qty - reserved_qty
                unit_cost = product_info.get('standard_price', 0)
                total_cost = on_hand_qty * unit_cost

                report_data.append({
                    'Product_Name': product_info.get('display_name', 'N/A'),
                    'Barcode': product_info.get('barcode', ''),
                    'Branch_Name': branch_name,
                    'Category': product_info.get('categ_id')[1] if isinstance(product_info.get('categ_id'), list) else 'N/A',
                    'Qty_On_Hand': float(on_hand_qty),
                    'Reserved_Qty': float(reserved_qty),
                    'Available_Qty': float(available_qty),
                    'Unit_Cost': float(unit_cost),
                    'Total_Cost': float(total_cost),
                    'Last_Updated': current_timestamp
                })

            final_df = pd.DataFrame(report_data)
            logging.info(f"✅ Assembled report with {len(final_df)} records.")

        # --- 7. Upload to BigQuery ---
        logging.info(f"📤 Uploading to BigQuery table: {DATASET_ID}.{STOCK_TABLE}")
        
        # --- MODIFICATION: Automated authentication ---
        # The client will automatically find credentials from the environment.
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        ensure_stock_table_exists(bq_client, DATASET_ID, STOCK_TABLE)
        
        # Upload data
        if not final_df.empty:
            # `to_gbq` will also find credentials automatically from the environment.
            final_df.to_gbq(
                destination_table=f"{DATASET_ID}.{STOCK_TABLE}",
                project_id=PROJECT_ID,
                if_exists='replace'
            )
            logging.info(f"✅ Successfully uploaded {len(final_df)} records to BigQuery table: {DATASET_ID}.{STOCK_TABLE}")
        else:
            logging.warning("⚠️ No data to upload - creating empty table structure.")
            
    except Exception as e:
        logging.error(f"❌ Critical error in main pipeline: {e}")
        # Raise the exception to make the GitHub Action fail
        raise
    
    logging.info("=== Stock Report Completed Successfully ===")

if __name__ == "__main__":
    main()
