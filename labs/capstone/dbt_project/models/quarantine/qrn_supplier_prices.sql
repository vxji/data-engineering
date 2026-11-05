-- Supplier rows Purchasing has to fix with the supplier: a cost that isn't positive, or an unknown product.
select
    p.supplier_id,
    p.sku,
    p.unit_cost_sar,
    p.valid_from,
    p.iso_week,
    case when p.unit_cost_sar is null or p.unit_cost_sar <= 0 then 'non-positive cost' else 'unknown SKU' end as reject_reason,
    'purchasing' as owner
from {{ ref('stg_supplier_prices') }} p
left join {{ ref('stg_products') }} k on k.sku = p.sku
where p.unit_cost_sar is null or p.unit_cost_sar <= 0 or k.sku is null
