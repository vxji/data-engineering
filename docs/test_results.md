# Test results

Latest full run: **55 of 58 checks passed**, 3 warnings, 0 blocking failures. `dbt build` also ran 3 unit tests and the reconciliation test `assert_payments_reconcile`, all passing.

Regenerate with `python capstone/pipeline.py --reset`; the run writes `test_results.json` to its folder.

## Warnings, each with an owner

| Asset | Check | Rows | Owner | Action |
|---|---|---:|---|---|
| `staging/stg_order_items` | `positive_stg_order_items_quantity` | 142 | Finance operations | Rows are in `quarantine.qrn_order_items` |
| `staging/stg_supplier_prices` | `positive_stg_supplier_prices_unit_cost_sar` | 4 | Purchasing | Rows are in `quarantine.qrn_supplier_prices` |
| `marts/fct_orders` | `assert_order_totals_match_payments` | 244 | Finance | Review payment differences |

## All checks

| Asset | Check | Result | Severity |
|---|---|---|---|
| `marts/dim_customers` | `not_null_dim_customers_customer_id` | pass | error |
| `marts/dim_customers` | `unique_dim_customers_customer_id` | pass | error |
| `marts/dim_date` | `not_null_dim_date_date_day` | pass | error |
| `marts/dim_date` | `unique_dim_date_date_day` | pass | error |
| `marts/dim_products` | `not_null_dim_products_product_id` | pass | error |
| `marts/dim_products` | `unique_dim_products_product_id` | pass | error |
| `marts/dim_stores` | `not_null_dim_stores_store_id` | pass | error |
| `marts/dim_stores` | `unique_dim_stores_store_id` | pass | error |
| `marts/fct_order_items` | `not_null_fct_order_items_customer_id` | pass | error |
| `marts/fct_order_items` | `positive_fct_order_items_quantity` | pass | error |
| `marts/fct_order_items` | `relationships_fct_order_items_customer_id__customer_id__ref_dim_customers_` | pass | error |
| `marts/fct_order_items` | `relationships_fct_order_items_order_date__date_day__ref_dim_date_` | pass | error |
| `marts/fct_order_items` | `relationships_fct_order_items_product_id__product_id__ref_dim_products_` | pass | error |
| `marts/fct_order_items` | `relationships_fct_order_items_store_id__store_id__ref_dim_stores_` | pass | error |
| `marts/fct_order_items` | `unique_fct_order_items_order_line_id` | pass | error |
| `marts/fct_orders` | `assert_order_totals_match_payments` | warn | warn |
| `marts/fct_orders` | `not_null_fct_orders_order_id` | pass | error |
| `marts/fct_orders` | `orders_volume` | pass | warn |
| `marts/fct_orders` | `relationships_fct_orders_customer_id__customer_id__ref_dim_customers_` | pass | error |
| `marts/fct_orders` | `unique_fct_orders_order_id` | pass | error |
| `marts/mart_daily_city_sales` | `unique_mart_daily_city_sales_order_date_city` | pass | error |
| `marts/mart_ops_daily` | `unique_mart_ops_daily_delivery_date_city` | pass | error |
| `ops/governed_views` | `no_pii_in_served_data` | pass | error |
| `quarantine/qrn_order_items` | `not_null_qrn_order_items_order_line_id` | pass | error |
| `quarantine/qrn_order_items` | `not_null_qrn_order_items_owner` | pass | error |
| `quarantine/qrn_order_items` | `unique_qrn_order_items_order_line_id` | pass | error |
| `quarantine/qrn_payments` | `not_null_qrn_payments_owner` | pass | error |
| `quarantine/qrn_payments` | `not_null_qrn_payments_payment_id` | pass | error |
| `quarantine/qrn_payments` | `unique_qrn_payments_payment_id` | pass | error |
| `quarantine/qrn_supplier_prices` | `not_null_qrn_supplier_prices_owner` | pass | error |
| `quarantine/qrn_supplier_prices` | `not_null_qrn_supplier_prices_reject_reason` | pass | error |
| `raw/customers` | `no_identifiers_in_warehouse` | pass | error |
| `raw/payments` | `payments_contract` | pass | error |
| `raw/payments` | `payments_freshness` | pass | warn |
| `staging/stg_customers` | `not_null_stg_customers_customer_id` | pass | error |
| `staging/stg_customers` | `not_null_stg_customers_person_id` | pass | error |
| `staging/stg_customers` | `not_null_stg_customers_person_token` | pass | error |
| `staging/stg_customers` | `unique_stg_customers_customer_id` | pass | error |
| `staging/stg_delivery_events` | `accepted_values_stg_delivery_events_event_type__assigned__picked_up__out_for_delivery__delivered__failed__cancelled` | pass | error |
| `staging/stg_delivery_events` | `not_null_stg_delivery_events_event_at_utc` | pass | error |
| `staging/stg_delivery_events` | `not_null_stg_delivery_events_event_id` | pass | error |
| `staging/stg_delivery_events` | `unique_stg_delivery_events_event_id` | pass | error |
| `staging/stg_order_items` | `not_null_stg_order_items_order_line_id` | pass | error |
| `staging/stg_order_items` | `positive_stg_order_items_quantity` | warn | warn |
| `staging/stg_order_items` | `unique_stg_order_items_order_line_id` | pass | error |
| `staging/stg_orders` | `accepted_values_stg_orders_status__placed__delivered__cancelled__refunded__failed_delivery` | pass | error |
| `staging/stg_orders` | `not_null_stg_orders_order_id` | pass | error |
| `staging/stg_orders` | `relationships_stg_orders_store_id__store_id__ref_stg_stores_` | pass | error |
| `staging/stg_orders` | `unique_stg_orders_order_id` | pass | error |
| `staging/stg_payments` | `not_null_stg_payments_payment_id` | pass | error |
| `staging/stg_payments` | `unique_stg_payments_payment_id` | pass | error |
| `staging/stg_products` | `not_null_stg_products_product_id` | pass | error |
| `staging/stg_products` | `unique_stg_products_product_id` | pass | error |
| `staging/stg_products` | `unique_stg_products_sku` | pass | error |
| `staging/stg_stores` | `not_null_stg_stores_store_id` | pass | error |
| `staging/stg_stores` | `unique_stg_stores_store_id` | pass | error |
| `staging/stg_supplier_prices` | `not_null_stg_supplier_prices_unit_cost_sar` | pass | error |
| `staging/stg_supplier_prices` | `positive_stg_supplier_prices_unit_cost_sar` | warn | warn |
