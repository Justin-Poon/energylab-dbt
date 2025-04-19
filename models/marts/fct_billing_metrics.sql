with invoices as (
    select *
    from {{ ref('fct_invoices') }}
)

select 
    -- Use DuckDB's strftime/strptime or date_trunc for month extraction
    strftime(billing_end_date, '%Y-%m') as billing_month,
    
    count(*) as count_invoices,
    sum(invoice_amount) as total_invoiced_amount,
    sum(total_amount_paid) as total_paid_amount,
    sum(outstanding_amount) as total_outstanding_amount,
    count(case when outstanding_amount <= 0 then invoice_id end) as count_paid_invoices,

    -- Collection Rate
    case 
        when sum(invoice_amount) > 0 then sum(total_amount_paid) / sum(invoice_amount)
        else 0
    end as collection_rate,
    
    -- Average Days To Pay (for fully paid invoices)
    avg(days_to_pay) as avg_days_to_pay

from invoices
group by 1
order by 1 