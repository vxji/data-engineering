-- Every raw payment is in staging or in quarantine, exactly once. Returns a row if any is lost.
{{ config(severity='error') }}

with counts as (
    select
        (select count(*) from {{ source('raw', 'payments') }})   as raw_rows,
        (select count(*) from {{ ref('stg_payments') }})         as clean_rows,
        (select count(*) from {{ ref('qrn_payments') }})         as quarantined_rows
)

select * from counts where raw_rows <> clean_rows + quarantined_rows
