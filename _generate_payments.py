import pandas as pd
import random
from datetime import date, timedelta
import os # Import os for path joining

# --- Configuration ---
workspace_root = 'energylab' # Define workspace root if needed, or adjust paths
invoice_seed_file = os.path.join(workspace_root, 'seeds', 'sample_billing_invoices.csv')
payment_seed_file = os.path.join(workspace_root, 'seeds', 'sample_payments.csv')
num_payments_to_generate = 250 # Number of payments to generate (can be <= number of invoices)

# --- Load Invoice Data ---
print(f"Reading invoice data from {invoice_seed_file}...")
try:
    invoices_df = pd.read_csv(invoice_seed_file)
    print(f"Columns found in invoice file: {invoices_df.columns.tolist()}")

    # --- Define expected column names from CSV header ---
    customer_col = 'Customer_Num'
    amount_col = 'Total Charge'

    # --- Validate columns exist ---
    if customer_col not in invoices_df.columns:
        raise ValueError(f"'{customer_col}' column not found in invoice file.")
    if amount_col not in invoices_df.columns:
        raise ValueError(f"'{amount_col}' column not found in invoice file.")

    # --- Generate synthetic invoice_id (matching stg_invoices logic) ---
    # Add 1 because pandas index is 0-based, but row_number() is 1-based
    invoices_df['generated_invoice_id'] = [f"INV{i+1:04d}" for i in range(len(invoices_df))]

    # Create a dictionary for quick lookup: {generated_invoice_id: (amount, customer_id)}
    invoice_details = invoices_df.set_index('generated_invoice_id')[[amount_col, customer_col]].apply(tuple, axis=1).to_dict()
    print(f"Processed {len(invoice_details)} invoices.")

except FileNotFoundError:
    print(f"Error: Invoice seed file not found at {invoice_seed_file}")
    exit(1)
except Exception as e:
    print(f"Error processing invoice file: {e}")
    exit(1)

# --- Generate Payment Data ---
print(f"Generating payment data ({num_payments_to_generate} rows)...")
payments_data = []
start_payment_date = date(2023, 1, 15) # Payments start slightly after invoices

# Get a list of valid generated invoice IDs
valid_invoice_ids = list(invoice_details.keys())

if num_payments_to_generate > len(valid_invoice_ids):
    print(f"Warning: Trying to generate more payments ({num_payments_to_generate}) than available invoices ({len(valid_invoice_ids)}). Adjusting count.")
    num_payments_to_generate = len(valid_invoice_ids)

# Sample generated invoice IDs to create payments for
invoices_to_pay = random.sample(valid_invoice_ids, num_payments_to_generate)

for gen_invoice_id in invoices_to_pay:
    # Look up the invoice details
    invoice_amount, customer_id = invoice_details.get(gen_invoice_id)

    if invoice_amount is None or pd.isna(invoice_amount):
        print(f"Warning: Skipping payment for {gen_invoice_id} due to missing/invalid amount.")
        continue
    if customer_id is None or pd.isna(customer_id):
        print(f"Warning: Skipping payment for {gen_invoice_id} due to missing customer ID.")
        continue

    # Ensure invoice_amount is a float for calculations
    try:
        invoice_amount_float = float(invoice_amount)
    except ValueError:
        print(f"Warning: Skipping payment for {gen_invoice_id} due to non-numeric invoice amount: {invoice_amount}")
        continue
        
    # --- Modified Payment Amount Logic ---
    # 70% chance to pay exactly 100%
    if random.random() < 0.7:
        amount_paid = round(invoice_amount_float, 2)
    else:
        # 30% chance to pay between 70% and 100% (original logic)
        payment_ratio = random.uniform(0.7, 1.0)
        amount_paid = round(invoice_amount_float * payment_ratio, 2)
    # --- End Modified Logic ---

    if amount_paid < 0.01 and invoice_amount_float >= 0.01: # Ensure non-zero payment unless invoice was 0
        amount_paid = 0.01
    elif amount_paid < 0:
         amount_paid = 0 # Handle potential negative invoice amounts just in case

    # Generate payment date
    payment_dt = start_payment_date + timedelta(days=random.randint(0, 90))
    payment_date_str = payment_dt.strftime('%Y-%m-%d')

    payments_data.append({
        'customer_id': str(int(customer_id)), # Use Customer_Num, ensure string
        'invoice_id': gen_invoice_id,      # Use the generated INVxxxx ID
        'payment_date': payment_date_str,
        'amount_paid': amount_paid
    })

# --- Save Payment Data ---
if payments_data:
    payments_df = pd.DataFrame(payments_data, columns=['customer_id', 'invoice_id', 'payment_date', 'amount_paid'])
    print(f"Saving {len(payments_df)} payments to {payment_seed_file}...")
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(payment_seed_file), exist_ok=True)
        payments_df.to_csv(payment_seed_file, index=False, encoding='utf-8')
        print(f"Successfully generated {payment_seed_file}.")
    except Exception as e:
        print(f"Error writing payment file: {e}")
        exit(1)
else:
    print("No payment data generated.") 