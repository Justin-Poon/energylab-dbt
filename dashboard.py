import streamlit as st
import duckdb
import pandas as pd
import os


# --- Page Config (Must be the first Streamlit command!) ---
st.set_page_config(layout="wide")
# ---------------------------------------------------------

# Database path (relative to project root)
# Use os.path.abspath to ensure consistent path handling
DB_PATH = os.path.abspath('energylab.duckdb') 
# DB_WAL_PATH = DB_PATH + ".wal" # No longer needed for read-only

# --- Removed Clean Slate Logic ---
# --- Removed dbt Build Logic ---

# --- Database Connection --- 
@st.cache_resource(show_spinner=False) # Re-enabled cache, added show_spinner=False
def get_db_connection(): 
    # st.info(f"Attempting to establish DB connection to: {DB_PATH}") # Removed info
    if not os.path.exists(DB_PATH):
        st.error(f"Database file not found at {DB_PATH}. Please ensure 'energylab.duckdb' is in the repository.") 
        st.stop() # Stop execution if DB not found
    try:
        # Connect read-only to pre-built database
        connection = duckdb.connect(DB_PATH, read_only=True)
        # st.success("Database connection established.") # Removed success
        return connection
    except Exception as e:
         # Simplified error handling for connection failure
         st.error(f"Failed to connect to database at {DB_PATH}: {e}")
         st.stop() # Stop execution on connection error

# Function to load monthly metrics data from DuckDB
# @st.cache_data # <-- Temporarily commented out for debugging
def load_monthly_metrics(con):
    if con is None: return pd.DataFrame() # Handle failed connection
    try:
        # st.info(f"Loading monthly metrics using existing connection...") # Removed info
        df = con.execute("SELECT * FROM main.fct_billing_metrics ORDER BY billing_month").df()
        # con.close() # Remove close - connection managed by Streamlit cache
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
        # st.info(f"Loading invoice data using existing connection...") # Removed info
        df = con.execute("SELECT * FROM main.fct_invoices ORDER BY customer_id, invoice_id").df()
        # con.close() # Remove close - connection managed by Streamlit cache
        return df
    except Exception as e:
        st.error(f"Error loading invoice data: {e}")
        return pd.DataFrame()

# --- Main App Logic --- 
# Get the shared database connection (no build step)
con = get_db_connection()

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

# --- Removed dbt build logic and related imports --- 