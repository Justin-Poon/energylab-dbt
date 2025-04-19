with invoice_payments as (
    select *
    from {{ ref('int_invoice_payments') }}
),

last_payment_dates as (
    select
        invoice_id,
        max(payment_date) as last_payment_date
    from {{ ref('stg_payments') }}
    group by 1
)

select
    ip.invoice_id,
    ip.customer_id,
    ip.billing_start_date,
    ip.billing_end_date,
    ip.invoice_amount,
    ip.total_amount_paid,
    ip.outstanding_amount,
    -- Determine if the invoice is currently overdue
    case 
        when ip.outstanding_amount > 0 and ip.billing_end_date < current_date then true
        else false 
    end as is_overdue,
    -- Calculate days to pay only if fully paid
    case
        when ip.outstanding_amount <= 0 then date_diff('day', ip.billing_end_date, lpd.last_payment_date)
        else null
    end as days_to_pay

from invoice_payments ip
left join last_payment_dates lpd on ip.invoice_id = lpd.invoice_id 