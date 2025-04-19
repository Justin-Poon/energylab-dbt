with invoices as (
    select *
    from {{ ref('stg_invoices') }}
),

payments as (
    select 
        invoice_id,
        sum(amount_paid) as total_amount_paid
    from {{ ref('stg_payments') }}
    group by 1
)

select
    i.invoice_id,
    i.customer_id,
    i.billing_start_date,
    i.billing_end_date,
    i.amount as invoice_amount,
    coalesce(p.total_amount_paid, 0) as total_amount_paid,
    (i.amount - coalesce(p.total_amount_paid, 0)) as outstanding_amount
    -- Add other invoice columns if needed
    -- i.prev_meter_reading,
    -- i.curr_meter_reading,
    -- i.usage_kwh
from invoices i
left join payments p on i.invoice_id = p.invoice_id 