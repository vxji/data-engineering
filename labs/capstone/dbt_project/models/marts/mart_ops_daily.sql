-- One row per Riyadh delivery day and city: the operations view. Event data starts on 24 September.
select
    ({{ to_riyadh('o.delivered_at_utc') }})::date                         as delivery_date,
    s.city,
    count(*)                                                              as delivered_orders,
    round(avg(o.delivery_minutes), 1)                                     as avg_delivery_minutes,
    round(quantile_cont(o.delivery_minutes, 0.9), 1)                      as p90_delivery_minutes,
    round(avg(o.within_sla::int), 3)                                      as sla_rate,
    count(*) filter (where not o.within_sla)                              as sla_breaches
from {{ ref('fct_orders') }} o
join {{ ref('dim_stores') }} s using (store_id)
where o.delivered_at_utc is not null
group by all
