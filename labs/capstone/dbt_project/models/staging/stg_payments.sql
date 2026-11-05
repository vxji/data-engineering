-- One row per valid payment attempt (failed attempts are real rows). Non-positive amounts go to
-- quarantine.qrn_payments instead: a payment can't be zero or negative, refunds are a status.
select
    payment_id,
    order_id,
    amount,
    currency,
    method,
    wallet_provider,
    status,
    card_last4,
    created_at::timestamptz at time zone 'UTC'   as created_at_utc,
    updated_at::timestamptz at time zone 'UTC'   as updated_at_utc
from {{ source('raw', 'payments') }}
where amount > 0
