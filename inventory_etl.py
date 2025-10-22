# inventory_etl_final.py
# هذا الكود يقوم بسحب بيانات المخزون بالكامل مباشرة من نظام Odoo
# ويرفعها إلى BigQuery، وهو مصمم للعمل بشكل آمن مع GitHub Actions.

import os
import requests
import pandas as pd
import logging
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- 1. SECURE METHOD: Load settings from environment variables ---
# --- ١. الطريقة الآمنة: تحميل الإعدادات من متغيرات البيئة ---
ODOO_URL = os.environ.get("ODOO_URL")
ODOO_DB = os.environ.get("ODOO_DB")
ODOO_USERNAME = os.environ.get("ODOO_USERNAME")
ODOO_PASSWORD = os.environ.get("ODOO_PASSWORD")

# --- 2. BigQuery Settings ---
# --- ٢. إعدادات BigQuery ---
PROJECT_ID = "spartan-cedar-467808-p9"
DATASET_ID = "Orders"
STOCK_TABLE = "stock_data"

# --- 3. Logging Setup ---
# --- ٣. إعداد نظام تسجيل الأنشطة ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# --- Core Odoo Functions ---

def get_odoo_session():
    """Authenticates with Odoo and returns a session object and user ID."""
    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD]):
        logging.error("❌ Odoo environment variables not set correctly.")
        logging.error("Required: ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD")
        return None, None

    session = requests.Session()
    auth_url = f"{ODOO_URL}/web/session/authenticate"
    payload = {"jsonrpc": "2.0", "method": "call", "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}}
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
        logging.error(f"Odoo connection error: {e}")
        return None, None

def call_odoo(session, uid, model, method, args=[], kwargs={}, timeout=600):
    """Makes a JSON-RPC call to Odoo with proper error handling."""
    rpc_url = f"{ODOO_URL}/jsonrpc"
    payload = {"jsonrpc": "2.0", "method": "call", "params": {"service": "object", "method": "execute_kw", "args": [ODOO_DB, uid, ODOO_PASSWORD, model, method, args, kwargs]}}
    try:
        response = session.post(rpc_url, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()
        if 'error' in result:
            logging.error(f"RPC Error on {model}.{method}: {result['error']['data']['message']}")
            return []
        return result.get('result', [])
    except Exception as e:
        logging.error(f"RPC call to {model}.{method} failed: {e}")
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
        logging.info(f"🔧 Table {table_id} not found, creating it with the new schema...")
        schema = [
            bigquery.SchemaField("Product_Name", "STRING"),
            bigquery.SchemaField("Barcode", "STRING"),
            bigquery.SchemaField("Branch_ID", "INTEGER"),
            bigquery.SchemaField("Branch_Name", "STRING"),
            bigquery.SchemaField("Location_ID", "INTEGER"),
            bigquery.SchemaField("Location_Full_Name", "STRING"),
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

# --- Main Logic ---
def main():
    """Main ETL pipeline function."""
    logging.info("=== Starting Comprehensive Stock ETL (Odoo to BigQuery) ===")
    
    session, uid = get_odoo_session()
    if not session:
        exit(1)

    logging.warning("⏳ This is a large data operation and may take several minutes.")

    try:
        # --- 2. Fetch Branches and Locations Directly from Odoo ---
        logging.info("🏢 Fetching warehouse/branch information...")
        warehouses = call_odoo(session, uid, "stock.warehouse", "search_read", [], {"fields": ["id", "name"]})
        branch_map = {w['id']: w['name'] for w in warehouses} if warehouses else {}

        logging.info("📍 Fetching internal stock locations (excluding 'EXPR')...")
        # Define the domain to filter locations
        location_domain = [
            ('usage', '=', 'internal'),
            ('complete_name', 'not ilike', 'EXPR')
        ]
        locations = call_odoo(session, uid, "stock.location", "search_read", 
                              [location_domain], 
                              {"fields": ["id", "complete_name", "warehouse_id"]})
        
        if not locations:
            logging.error("❌ No internal locations found after filtering. Cannot proceed.")
            exit(1)
            
        target_location_ids = [loc['id'] for loc in locations]
        location_map = {loc['id']: {
            'complete_name': loc['complete_name'], 
            'warehouse_id': loc['warehouse_id'][0] if isinstance(loc.get('warehouse_id'), list) else None
        } for loc in locations}
        
        logging.info(f"✅ Found {len(branch_map)} branches and {len(target_location_ids)} relevant internal locations.")

        # --- 3. Fetch Stock Quants ---
        logging.info("📦 Fetching stock levels for all relevant internal locations...")
        quant_domain = [('location_id', 'in', target_location_ids)]
        quant_fields = ['product_id', 'location_id', 'quantity', 'reserved_quantity']
        stock_quants = call_odoo(session, uid, "stock.quant", "search_read", 
                                  [quant_domain], {"fields": quant_fields, "limit": 500000})

        if not stock_quants:
            logging.warning("⚠️ No stock found. The process will finish, and the BigQuery table will be empty.")
            final_df = pd.DataFrame() # Create empty DF to ensure table replacement
        else:
            logging.info(f"✅ Found {len(stock_quants)} total stock records.")
            
            # --- 4. Fetch Product Details ---
            product_ids = list(set([q['product_id'][0] for q in stock_quants if q.get('product_id')]))
            logging.info(f"📝 Fetching details for {len(product_ids)} unique products...")
            product_fields = ['id', 'display_name', 'barcode', 'categ_id', 'standard_price']
            product_data = fetch_in_batches(session, uid, "product.product", product_ids, product_fields)
            product_map = {p['id']: p for p in product_data}

            # --- 5. Assemble the Final Report ---
            logging.info("⚙️ Assembling the final report...")
            report_data = []
            current_timestamp = datetime.now(timezone.utc)
            
            for quant in stock_quants:
                if not quant.get('product_id'): continue
                
                product_id = quant['product_id'][0]
                product_info = product_map.get(product_id, {})
                
                location_id = quant['location_id'][0]
                location_info = location_map.get(location_id, {})
                warehouse_id = location_info.get('warehouse_id')
                branch_name = branch_map.get(warehouse_id, 'N/A')
                location_full_name = location_info.get('complete_name', 'N/A')

                on_hand_qty = quant.get('quantity', 0)
                reserved_qty = quant.get('reserved_quantity', 0)
                available_qty = on_hand_qty - reserved_qty
                unit_cost = product_info.get('standard_price', 0)
                total_cost = on_hand_qty * unit_cost
                
                # FIX: Ensure barcode is a string to prevent type errors
                barcode_val = product_info.get('barcode')
                barcode_str = str(barcode_val) if barcode_val else ''

                report_data.append({
                    'Product_Name': product_info.get('display_name', 'N/A'),
                    'Barcode': barcode_str,
                    'Branch_ID': warehouse_id,
                    'Branch_Name': branch_name,
                    'Location_ID': location_id,
                    'Location_Full_Name': location_full_name,
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

        # --- 6. Upload to BigQuery ---
        logging.info(f"📤 Uploading data to BigQuery table: {DATASET_ID}.{STOCK_TABLE}")
        bq_client = bigquery.Client(project=PROJECT_ID)
        ensure_stock_table_exists(bq_client, DATASET_ID, STOCK_TABLE)
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        if not final_df.empty:
            job = bq_client.load_table_from_dataframe(final_df, f"{PROJECT_ID}.{DATASET_ID}.{STOCK_TABLE}", job_config=job_config)
            job.result()  # Wait for the job to complete
            logging.info(f"✅ Successfully uploaded {len(final_df)} records to BigQuery.")
        else:
            # If there's no stock, we still truncate the table to reflect the current state.
            logging.info("No stock data to upload. Ensuring table is empty.")
            bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{STOCK_TABLE}`").result()

    except Exception as e:
        logging.error(f"❌ Critical error in main pipeline: {e}")
        raise
    
    logging.info("=== Stock ETL Completed Successfully ===")

if __name__ == "__main__":
    main()

