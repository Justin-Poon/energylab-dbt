import streamlit as st
import duckdb
import pandas as pd
# import subprocess # No longer needed
import os
# import sys # No longer needed
# import shlex # No longer needed
from dbt.cli.main import dbtRunner, dbtRunnerResult # Import dbtRunner
import time # Import time for potential sleep

# --- Page Config (Must be the first Streamlit command!) ---
st.set_page_config(layout="wide")
# ---------------------------------------------------------

# Database path (relative to project root)
# DB_PATH = 'energylab.duckdb' # Old path
DB_PATH = '/tmp/energylab.duckdb' # Use the same absolute path as profiles.yml
DB_WAL_PATH = DB_PATH + ".wal" # WAL file path
# DBT_PROJECT_DIR = 'energylab' # No longer needed

# --- Force Clean Slate on Start ---
st.info(f"Checking for and deleting existing DB files: {DB_PATH}*")
try:
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        st.info(f"Deleted {DB_PATH}") # Use info level for routine deletion
    if os.path.exists(DB_WAL_PATH):
        os.remove(DB_WAL_PATH)
        st.info(f"Deleted {DB_WAL_PATH}")
except OSError as e:
    st.error(f"Error deleting existing database files: {e}. App may fail.")
# --- End Clean Slate ---

# --- dbt Build Logic --- 
@st.cache_resource # Build should only run once per session now
def build_dbt_database():
    db_abs_path = os.path.abspath(DB_PATH)
    st.warning(f"Running dbt commands to build database at {db_abs_path}...") # Removed initial check msg
    build_success = False
    try:
        # Store/clear env vars
        old_project_dir = os.environ.pop('DBT_PROJECT_DIR', None)
        old_profiles_dir = os.environ.pop('DBT_PROFILES_DIR', None)
        if old_project_dir or old_profiles_dir:
            st.info("Cleared existing DBT environment variables.")

        dbt = dbtRunner()
        # Add --debug flag for verbose logging
        deps_args = ["deps", "--project-dir", ".", "--debug"]
        seed_args = ["seed", "--project-dir", ".", "--debug"]
        run_args = ["run", "--project-dir", ".", "--debug"]

        st.info(f"Running dbt deps...") 
        deps_res: dbtRunnerResult = dbt.invoke(deps_args)
        if not deps_res.success:
            st.error("dbt deps failed.")
            if deps_res.exception: st.error(f"dbt deps exception: {deps_res.exception}")
            if deps_res.result: st.error(f"dbt deps result: {deps_res.result}")
        else:
            st.success("dbt deps completed.")
            st.info(f"Running dbt seed...") 
            seed_res: dbtRunnerResult = dbt.invoke(seed_args)
            if not seed_res.success:
                st.error("dbt seed failed.")
                if seed_res.exception: st.error(f"dbt seed exception: {seed_res.exception}")
                if seed_res.result: st.error(f"dbt seed result: {seed_res.result}") 
            else:
                st.success("dbt seed completed.")
                st.info(f"Running dbt run...") 
                run_res: dbtRunnerResult = dbt.invoke(run_args)
                if not run_res.success:
                    st.error("dbt run failed.")
                    if run_res.exception: st.error(f"dbt run exception: {run_res.exception}")
                    if run_res.result: st.error(f"dbt run result: {run_res.result}")
                else:
                    st.success("dbt run completed.")
                    build_success = True
                    # Optional: Add a small delay 
                    # time.sleep(1) 
    except Exception as e:
            st.error(f"An unexpected error occurred during dbt execution: {e}")
    finally:
            # Restore env vars 
            if old_project_dir:
                os.environ['DBT_PROJECT_DIR'] = old_project_dir
            if old_profiles_dir:
                os.environ['DBT_PROFILES_DIR'] = old_profiles_dir
    
    if build_success:
            st.info("dbt build process finished.")
            if not os.path.exists(db_abs_path):
                st.error("Database file still does not exist after dbt reported success.")
                build_success = False # Mark as failed if file missing
    else:
            st.error("Database build failed during execution.")
            
    return build_success # Return status 

# --- Database Connection --- 
@st.cache_resource # Cache the connection separately
def get_db_connection(build_outcome):
    # Only attempt connection if build reported success
    if not build_outcome:
        st.warning("Skipping DB connection attempt because build failed.")
        return None 
        
    db_connect_path = os.path.abspath(DB_PATH)
    st.info(f"Attempting to establish DB connection to: {db_connect_path}")
    if not os.path.exists(db_connect_path):
        st.error(f"Database file not found at {db_connect_path} when trying to connect (unexpected)." ) 
        return None
    try:
        # Add short delay before connecting read-only
        time.sleep(0.5) 
        connection = duckdb.connect(db_connect_path, read_only=True)
        st.success("Database connection established.")
        return connection
    except Exception as e:
         st.error(f"Failed to connect to database at {db_connect_path}: {e}")
         return None

# Function to load monthly metrics data from DuckDB
# @st.cache_data # <-- Temporarily commented out for debugging
def load_monthly_metrics(con):
    if con is None: return pd.DataFrame() # Handle failed connection
    try:
        st.info(f"Loading monthly metrics using existing connection...")
        df = con.execute("SELECT * FROM main.fct_billing_metrics ORDER BY billing_month").df()
        # con.close() # Remove close
        df['billing_month'] = pd.to_datetime(df['billing_month'] + '-01')
        return df
    except Exception as e:
        st.error(f"Error loading monthly metrics: {e}")
        return pd.DataFrame() 

# Function to load individual invoice data from DuckDB
# @st.cache_data # <-- Temporarily commented out for debugging
def load_invoice_data(con):
    if con is None: return pd.DataFrame() # Handle failed connection
    try:
        st.info(f"Loading invoice data using existing connection...")
        df = con.execute("SELECT * FROM main.fct_invoices ORDER BY customer_id, invoice_id").df()
        # con.close() # Remove close
        return df
    except Exception as e:
        st.error(f"Error loading invoice data: {e}")
        return pd.DataFrame()

# --- Main App Logic --- 
# Force build DB on first run of session
build_status = build_dbt_database()

# Get the shared database connection (depends on build status)
con = get_db_connection(build_status)

# Load the data using the connection
df_metrics = load_monthly_metrics(con)
df_invoices = load_invoice_data(con) 

st.title("Billing Dashboard") # Updated title slightly

if not df_metrics.empty:
    # --- KPIs --- 
    st.subheader("Monthly Summary KPIs") # More specific subheader
    total_invoiced = df_metrics['total_invoiced_amount'].sum()
    total_collected = df_metrics['total_paid_amount'].sum()
    overall_collection_rate = total_collected / total_invoiced if total_invoiced else 0
    avg_days = df_metrics['avg_days_to_pay'].mean() # Simple average over months
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Invoiced (All Months)", f"${total_invoiced:,.2f}")
    col2.metric("Overall Collection Rate", f"{overall_collection_rate:.1%}")
    col3.metric("Avg. Days to Pay (Paid Invoices)", f"{avg_days:.1f}")

    st.divider()
    
    # --- Charts --- 
    st.subheader("Monthly Trends")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.line_chart(df_metrics, x='billing_month', y=['total_invoiced_amount', 'total_paid_amount'])
        
    with col_chart2:
        st.line_chart(df_metrics, x='billing_month', y='collection_rate')

    st.divider()

    # --- Monthly Data Table ---
    st.subheader("Monthly Aggregated Data") # More specific subheader
    st.dataframe(df_metrics.style.format({
        'billing_month': '{:%Y-%m}',
        'total_invoiced_amount': '${:,.2f}',
        'total_paid_amount': '${:,.2f}',
        'total_outstanding_amount': '${:,.2f}',
        'collection_rate': '{:.1%}',
        'avg_days_to_pay': '{:.1f}'
    }), use_container_width=True)
else:
    st.warning("Could not load monthly metrics data. Check logs.")

# --- Customer Invoice Detail Section ---
st.divider()
st.subheader("Customer Invoice Details")

if not df_invoices.empty:
    # Display the invoice data table
    st.dataframe(df_invoices.style.format({ # Apply formatting if desired
        # Example formatting (adjust column names as needed)
        'invoice_amount': '${:,.2f}',
        'total_amount_paid': '${:,.2f}',
        'outstanding_amount': '${:,.2f}'
        # 'billing_start_date': '{:%Y-%m-%d}', 
        # 'billing_end_date': '{:%Y-%m-%d}'
    }), use_container_width=True)
else:
    st.warning("Could not load invoice data. Check logs.") 