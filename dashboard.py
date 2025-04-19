import streamlit as st
import duckdb
import pandas as pd
# import subprocess # No longer needed
import os
# import sys # No longer needed
# import shlex # No longer needed
from dbt.cli.main import dbtRunner, dbtRunnerResult # Import dbtRunner

# --- Page Config (Must be the first Streamlit command!) ---
st.set_page_config(layout="wide")
# ---------------------------------------------------------

# Database path (relative to project root)
DB_PATH = 'energylab.duckdb'
DBT_PROJECT_DIR = 'energylab' # Specify dbt project directory relative to script

# --- Function to run dbt commands using Python API ---
# Removed run_dbt_command function

# --- Run dbt if database doesn't exist ---
# Cache this step to ensure it only runs once per session if DB is missing initially
@st.cache_resource
def build_dbt_database():
    # Get the absolute path to the database file, assuming it's in the root dir
    db_abs_path = os.path.abspath(DB_PATH)
    # Get the absolute path to the dbt project directory
    dbt_project_abs_path = os.path.abspath(DBT_PROJECT_DIR)
    
    if not os.path.exists(db_abs_path):
        st.warning(f"{db_abs_path} not found. Running dbt commands via Python API to build it...")
        
        original_cwd = os.getcwd()
        st.info(f"Original working directory: {original_cwd}")
        
        try:
            st.info(f"Changing working directory to: {dbt_project_abs_path}")
            os.chdir(dbt_project_abs_path)
            
            # Initialize dbtRunner *after* changing directory
            dbt = dbtRunner()
            
            # Define commands, adding --profiles-dir pointing to parent (original CWD)
            seed_args = ["seed", "--profiles-dir", ".."] 
            run_args = ["run", "--profiles-dir", ".."] 
            
            st.info(f"Running dbt seed in {os.getcwd()} (profiles dir: '..')...") 
            seed_res: dbtRunnerResult = dbt.invoke(seed_args)
            if seed_res.success:
                st.success("dbt seed completed successfully.")
                
                st.info(f"Running dbt run in {os.getcwd()} (profiles dir: '..')...") 
                run_res: dbtRunnerResult = dbt.invoke(run_args)
                if run_res.success:
                    st.success("dbt run completed successfully. Database should be ready.")
                else:
                    st.error("dbt run failed. Cannot load data.")
                    if run_res.exception:
                        st.error(f"dbt run exception: {run_res.exception}")
                    if run_res.result:
                        st.error(f"dbt run result: {run_res.result}")
            else:
                st.error("dbt seed failed. Cannot load data.")
                if seed_res.exception:
                    st.error(f"dbt seed exception: {seed_res.exception}")
                if seed_res.result:
                    st.error(f"dbt seed result: {seed_res.result}") 
                    
        except FileNotFoundError:
             st.error(f"Error: Could not change directory to {dbt_project_abs_path}. Does it exist?")
        except Exception as e:
             st.error(f"An unexpected error occurred during dbt execution: {e}")
        finally:
             # --- CRITICAL: Change back to original directory --- 
             st.info(f"Changing working directory back to: {original_cwd}")
             os.chdir(original_cwd)
             st.info(f"Current working directory: {os.getcwd()}")
             # --- End Change Back --- 
    else:
        # Use absolute path for check and info message
        st.info(f"Found existing database: {db_abs_path}")

# --- Build dbt database if necessary ---
build_dbt_database()

# Function to load monthly metrics data from DuckDB
@st.cache_data # Cache the data to avoid reloading on every interaction
def load_monthly_metrics():
    try:
        # Use absolute path for connection
        con = duckdb.connect(os.path.abspath(DB_PATH), read_only=True)
        # Query for monthly metrics
        df = con.execute("SELECT * FROM main.fct_billing_metrics ORDER BY billing_month").df()
        con.close()
        # Convert month string to datetime for charting
        df['billing_month'] = pd.to_datetime(df['billing_month'] + '-01')
        return df
    except Exception as e:
        st.error(f"Error loading monthly metrics from {DB_PATH}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# Function to load individual invoice data from DuckDB
@st.cache_data
def load_invoice_data():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        # Query for individual invoice details
        df = con.execute("SELECT * FROM main.fct_invoices ORDER BY customer_id, invoice_id").df()
        con.close()
        # Convert dates if needed (assuming they are already date/datetime)
        # Example: df['billing_start_date'] = pd.to_datetime(df['billing_start_date'])
        return df
    except Exception as e:
        st.error(f"Error loading invoice data from {DB_PATH}: {e}")
        return pd.DataFrame()

# Load the data
df_metrics = load_monthly_metrics()
df_invoices = load_invoice_data() # Load invoice data

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
    st.warning("Could not load monthly metrics data. Did you run `dbt build`?")

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
    st.warning("Could not load invoice data. Did you run `dbt build`?") 