-- Order lines that fail validation are kept here, with the reason, instead of silently
-- disappearing or breaking the build. The owner reviews and empties this table.
select
    *,
    'non-positive quantity' as reject_reason,
    'finance-operations'    as owner
from {{ ref('stg_order_items') }}
where quantity <= 0
