import duckdb
import sys
import pandas as pd

db_file = 'energylab.duckdb'
# Query fct_invoices to check outstanding amounts
sql_query = '''
SELECT
    invoice_id,
    customer_id,
    invoice_amount,
    total_amount_paid,
    outstanding_amount,
    days_to_pay -- Also check days_to_pay
FROM main.fct_invoices
ORDER BY invoice_id
LIMIT 50; -- Show first 50 invoices
'''

print(f"Connecting to {db_file}...")
try:
    con = duckdb.connect(db_file, read_only=True)
    print(f"Executing query on main.fct_invoices...")
    result = con.execute(sql_query).df()
    print("\nQuery Result (fct_invoices Sample):")
    if result.empty:
        print("No invoice data found in fct_invoices.")
    else:
        pd.set_option('display.max_rows', 100)
        print(result.to_string())
    con.close()
except Exception as e:
    print(f"\nError executing query: {e}")
    sys.exit(1)

sys.exit(0) 