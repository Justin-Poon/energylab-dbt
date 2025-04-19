import streamlit as st
import duckdb
import pandas as pd
import subprocess
import os
import sys # Import sys
import shlex # Import shlex for safer command splitting

# --- Page Config (Must be the first Streamlit command!) ---
st.set_page_config(layout="wide")
# ---------------------------------------------------------

# Database path (relative to project root)
DB_PATH = 'energylab.duckdb'
DBT_PROJECT_DIR = 'energylab' # Specify dbt project directory relative to script

# --- Function to run dbt commands ---
def run_dbt_command(command_list):
    command_str = " ".join(map(shlex.quote, command_list)) # For display purposes
    st.info(f"Running: {command_str}...")
    try:
        # Use shell=False and pass command as list
        result = subprocess.run(command_list, check=True, capture_output=True, text=True,
                                cwd=os.path.dirname(__file__) 
                                )
        # Display stdout in an expander
        with st.expander(f"Output for: {command_str}", expanded=False):
             st.code(result.stdout, language=None) # Display raw output
        if result.stderr:
             st.text("Error output (stderr):")
             st.code(result.stderr, language=None) # Show stderr output as well
        st.success(f"'{command_str}' completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        st.error(f"Error running command: {command_str}")
        st.error(f"Return code: {e.returncode}")
        # Display output/error in expanders even on failure
        with st.expander(f"Failed Output for: {command_str}", expanded=True):
            st.text("Standard Output:")
            st.code(e.stdout or "(No stdout)", language=None)
            st.text("Error Output (stderr):")
            st.code(e.stderr or "(No stderr)", language=None)
        return False
    except FileNotFoundError: # Catch specific error if python/dbt module not found
        st.error(f"Error: Command not found when trying to run '{command_str}'. Is dbt installed correctly?")
        return False
    except Exception as e:
        st.error(f"An unexpected error occurred while running dbt: {e}")
        return False

# --- Run dbt if database doesn't exist ---
# Cache this step to ensure it only runs once per session if DB is missing initially
@st.cache_resource
def build_dbt_database():
    if not os.path.exists(DB_PATH):
        st.warning(f"{DB_PATH} not found. Running dbt commands to build it...")
        # Use sys.executable to get path to current python interpreter
        python_executable = sys.executable 
        
        # Construct commands as lists
        seed_command_list = [
            python_executable, 
            "-m", "dbt", 
            "seed", 
            "--project-dir", DBT_PROJECT_DIR
        ]
        run_command_list = [
            python_executable, 
            "-m", "dbt", 
            "run", 
            "--project-dir", DBT_PROJECT_DIR
        ]
        
        seed_success = run_dbt_command(seed_command_list)
        if seed_success:
            run_success = run_dbt_command(run_command_list)
            if run_success:
                st.success("dbt build process completed. Database should be ready.")
            else:
                st.error("dbt run failed. Cannot load data.")
        else:
            st.error("dbt seed failed. Cannot load data.")
    else:
        st.info(f"Found existing database: {DB_PATH}")

# --- Build dbt database if necessary ---
build_dbt_database()

# Function to load monthly metrics data from DuckDB
@st.cache_data # Cache the data to avoid reloading on every interaction
def load_monthly_metrics():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
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