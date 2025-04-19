with source as (

    select * from {{ ref('sample_billing_invoices') }}

),

renamed as (

    select
        'INV' || lpad(row_number() over (order by "Customer_Num", "Billing Start Date")::text, 4, '0') as invoice_id,
        "Customer_Num"::text as customer_id,
        cast("Billing Start Date" as date) as billing_start_date,
        cast("Billing End Date" as date) as billing_end_date,
        cast("Previous Meter Reading" as integer) as prev_meter_reading,
        cast("Current Meter Reading" as integer) as curr_meter_reading,
        cast("Usage (kWh)" as numeric) as usage_kwh,
        cast("Total Charge" as numeric) as amount
    from source

)

select * from renamed
