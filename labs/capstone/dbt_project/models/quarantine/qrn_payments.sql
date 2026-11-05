-- Payments the contract rejects: kept with the reason and the owner, never in revenue.
select
    payment_id,
    order_id,
    amount,
    currency,
    method,
    status,
    created_at,
    updated_at,
    'non-positive amount'           as reject_reason,
    'payments-provider-integration' as owner
from {{ source('raw', 'payments') }}
where amount <= 0
