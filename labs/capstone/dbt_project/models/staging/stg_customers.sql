-- One row per customer account. Direct identifiers never reach the warehouse: they stay in the
-- vault, and person_token links the accounts of one person (see capstone/governance/privacy.py).
with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    min(customer_id) over (partition by person_token)         as person_id,     -- oldest account of this person
    person_token,
    email_domain,
    city,
    district,
    created_at::timestamp                                     as created_at,
    updated_at::timestamp                                     as updated_at,
    is_deleted = 1                                            as is_deleted
from source
