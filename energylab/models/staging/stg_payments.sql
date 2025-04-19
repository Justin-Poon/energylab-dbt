-- with source as (
-- 
--     -- select * from {{ source('seed', 'sample_payments') }}
--     select * from {{ ref('sample_payments') }} -- Use ref() for seeds in the project
-- 
-- ),
-- 
-- renamed as (
-- 
--     select
--         -- payment_id::text as payment_id, # REMOVED
--         customer_id::text as customer_id,
--         invoice_id::text as invoice_id,
--         cast(payment_date as date) as payment_date,
--         cast(amount_paid as numeric) as amount_paid
--     from source
-- 
-- )
-- 
-- select * from renamed

-- Simplified version:
with source as (
    select * from {{ ref('sample_payments') }}
)

-- Selecting from aliased CTE with qualified columns
select 
    src.customer_id::text as customer_id,
    replace(src.invoice_id, '_', '')::text as invoice_id,
    cast(src.payment_date as date) as payment_date,
    cast(src.amount_paid as numeric) as amount_paid
from source as src -- Alias the CTE
