-- Seed test data for validation_history demo
-- Run with: psql -d postgres -a -f src/app/backend/sql/seed_test_data.sql

-- Truncate tables in reverse dependency order and reset sequences
TRUNCATE control.validation_history RESTART IDENTITY CASCADE;
TRUNCATE control.entity_tags CASCADE;
TRUNCATE control.tags RESTART IDENTITY CASCADE;
TRUNCATE control.schedule_bindings RESTART IDENTITY CASCADE;
TRUNCATE control.triggers RESTART IDENTITY CASCADE;
TRUNCATE control.schedules RESTART IDENTITY CASCADE;
TRUNCATE control.compare_queries RESTART IDENTITY CASCADE;
TRUNCATE control.datasets RESTART IDENTITY CASCADE;
TRUNCATE control.type_transformations RESTART IDENTITY CASCADE;
TRUNCATE control.systems RESTART IDENTITY CASCADE;

-- 1) Systems
INSERT INTO control.systems (id, name, kind, catalog, created_by, updated_by)
VALUES 
  (1, 'warehouse-prod', 'databricks', 'dw_prod', 'system', 'system'),
  (2, 'analytics-prod', 'databricks', 'analytics_prod', 'system', 'system'),
  (4, 'crm-legacy', 'databricks', 'crm_legacy', 'system', 'system');

SELECT setval('control.systems_id_seq', (SELECT MAX(id) FROM control.systems));

-- 2) Datasets
INSERT INTO control.datasets (id, name, src_system_id, src_schema, src_table, tgt_system_id, tgt_schema, tgt_table, compare_mode, pk_columns, exclude_columns, created_by, updated_by)
VALUES 
  (83, 'SALES.ORDERS_FACT', 2, 'SALES', 'ORDERS_FACT', 1, 'SALES', 'ORDERS_FACT', 
   'primary_key', 
   ARRAY['ORDER_ID','LINE_ITEM_ID','PRODUCT_SKU','REGION_CODE','ORDER_DATE','FISCAL_PERIOD','CHANNEL_CODE'],
   ARRAY['ETL_LOAD_TS','ETL_UPDATE_TS','SRC_MODIFIED_TS','SRC_CREATED_TS'],
   'system', 'system'),
  (164, 'SUPPORT.TICKET_ASSIGNMENTS', 4, 'SUPPORT', 'TICKET_ASSIGNMENTS', 2, 'SUPPORT', 'TICKET_ASSIGNMENTS',
   'except_all',
   ARRAY[]::TEXT[],
   ARRAY['resolved_date','assigned_date'],
   'system', 'system');

SELECT setval('control.datasets_id_seq', (SELECT MAX(id) FROM control.datasets));

-- 3) Tags
INSERT INTO control.tags (name) VALUES ('primary_key'), ('except_all');

-- 4) Tag associations
INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
SELECT 'table', 83, id FROM control.tags WHERE name = 'primary_key';

INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
SELECT 'table', 164, id FROM control.tags WHERE name = 'except_all';

-- 5) Validation history records
INSERT INTO control.validation_history (
  id, trigger_id, entity_type, entity_id, entity_name, source, schedule_id,
  requested_by, requested_at, started_at, finished_at,
  source_system_id, target_system_id, source_system_name, target_system_name,
  source_table, target_table, sql_query, compare_mode, pk_columns, exclude_columns,
  status, schema_match, schema_details, row_count_source, row_count_target, row_count_match,
  rows_compared, rows_matched, rows_different, sample_differences,
  error_message, error_details, databricks_run_id, databricks_run_url, full_result, created_at
) VALUES 
(
  12415, 2921, 'table', 83, 'SALES.ORDERS_FACT', 'schedule', NULL,
  'system', '2026-01-14T09:24:01.481+00:00', '2026-01-14T09:24:27.672+00:00', '2026-01-14T09:24:47.836+00:00',
  2, 1, 'analytics-prod', 'warehouse-prod',
  'SALES.ORDERS_FACT', 'SALES.ORDERS_FACT', NULL,
  'primary_key',
  ARRAY['ORDER_ID','LINE_ITEM_ID','PRODUCT_SKU','REGION_CODE','ORDER_DATE','FISCAL_PERIOD','CHANNEL_CODE'],
  ARRAY['ETL_LOAD_TS','ETL_UPDATE_TS','SRC_MODIFIED_TS','SRC_CREATED_TS'],
  'failed', true,
  '{"columns_extra": [], "columns_matched": ["ORDER_ID", "LINE_ITEM_ID", "PRODUCT_SKU", "REGION_CODE", "ORDER_DATE", "FISCAL_PERIOD", "CHANNEL_CODE", "QUANTITY", "UNIT_PRICE", "DISCOUNT_PCT", "TAX_AMOUNT", "SHIPPING_COST", "TOTAL_AMOUNT", "CURRENCY_CODE", "CUSTOMER_ID", "SALES_REP_ID", "WAREHOUSE_ID", "FULFILLMENT_STATUS", "PAYMENT_METHOD", "PROMO_CODE", "IS_GIFT", "GIFT_MESSAGE", "PRIORITY_FLAG", "SHIP_METHOD", "TRACKING_NUMBER", "DELIVERY_DATE", "RETURN_FLAG"], "columns_missing": []}'::jsonb,
  113171, 113171, true,
  113171, 112878, 293,
  '[{"ORDER_ID": 78234561, "LINE_ITEM_ID": 1, "PRODUCT_SKU": "WH-BLK-XL-001", "REGION_CODE": "NA-WEST", "ORDER_DATE": "2025-12-15", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "ONLINE", "DESCRIPTION": "Premium warehouse black extra-large storage container with reinforced steel corners and weatherproof coating for outdoor industrial use in harsh climates"}, {"ORDER_ID": 78234562, "LINE_ITEM_ID": 2, "PRODUCT_SKU": "ACC-USB-C-PRO", "REGION_CODE": "NA-EAST", "ORDER_DATE": "2025-12-15", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "RETAIL"}, {"ORDER_ID": 78234563, "LINE_ITEM_ID": 1, "PRODUCT_SKU": "ELC-HDMI-4K", "REGION_CODE": "EMEA-UK", "ORDER_DATE": "2025-12-16", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "WHOLESALE", "DESCRIPTION": "High-speed braided HDMI 2.1 cable supporting 4K at 120Hz and 8K at 60Hz with dynamic HDR and eARC for premium home theater and gaming setups worldwide"}, {"ORDER_ID": 78234564, "LINE_ITEM_ID": 3, "PRODUCT_SKU": "OFF-CHAIR-ERG", "REGION_CODE": "APAC-AU", "ORDER_DATE": "2025-12-16", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "B2B"}, {"ORDER_ID": 78234565, "LINE_ITEM_ID": 1, "PRODUCT_SKU": "KIT-TOOL-SET", "REGION_CODE": "NA-CENTRAL", "ORDER_DATE": "2025-12-17", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "ONLINE"}, {"ORDER_ID": 78234566, "LINE_ITEM_ID": 2, "PRODUCT_SKU": "SPT-YOGA-MAT", "REGION_CODE": "LATAM-BR", "ORDER_DATE": "2025-12-17", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "MARKETPLACE", "DESCRIPTION": "Extra-thick non-slip yoga mat with alignment markers and carrying strap made from eco-friendly TPE material suitable for hot yoga and pilates classes"}, {"ORDER_ID": 78234567, "LINE_ITEM_ID": 1, "PRODUCT_SKU": "HOM-LAMP-LED", "REGION_CODE": "EMEA-DE", "ORDER_DATE": "2025-12-18", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "RETAIL"}, {"ORDER_ID": 78234568, "LINE_ITEM_ID": 4, "PRODUCT_SKU": "GAR-HOSE-50FT", "REGION_CODE": "NA-WEST", "ORDER_DATE": "2025-12-18", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "ONLINE"}, {"ORDER_ID": 78234569, "LINE_ITEM_ID": 1, "PRODUCT_SKU": "PET-BED-LRG", "REGION_CODE": "APAC-JP", "ORDER_DATE": "2025-12-19", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "B2B"}, {"ORDER_ID": 78234570, "LINE_ITEM_ID": 2, "PRODUCT_SKU": "TOY-LEGO-CITY", "REGION_CODE": "NA-EAST", "ORDER_DATE": "2025-12-19", "FISCAL_PERIOD": "Q4-2025", "CHANNEL_CODE": "WHOLESALE"}]'::jsonb,
  NULL, '{}'::jsonb, '795858107839121', 'https://example-databricks.cloud.databricks.com/jobs/123456789/runs/795858107839121', '{}'::jsonb,
  '2026-01-14T09:24:47.920+00:00'
),
(
  12448, 3025, 'table', 164, 'SUPPORT.TICKET_ASSIGNMENTS', 'schedule', NULL,
  'system', '2026-01-14T15:30:29.792+00:00', '2026-01-14T15:30:41.704+00:00', '2026-01-14T15:31:00.110+00:00',
  4, 2, 'crm-legacy', 'analytics-prod',
  'SUPPORT.TICKET_ASSIGNMENTS', 'SUPPORT.TICKET_ASSIGNMENTS', NULL,
  'except_all',
  ARRAY[]::TEXT[],
  ARRAY['resolved_date','assigned_date'],
  'failed', true,
  '{"columns_extra": [], "columns_matched": ["ticket_id", "assignment_id", "agent_id", "queue_name", "priority", "status", "category", "subcategory", "customer_tier", "sla_deadline_utc", "first_response_utc", "escalation_level"], "columns_missing": []}'::jsonb,
  2, 2, true,
  2, 0, 2,
  '[{"ticket_id": 982341, "assignment_id": 55012, "agent_id": "AGT-4421", "queue_name": "Billing-Tier2", "priority": "HIGH", "status": "OPEN", "category": "Payment", "subcategory": "Refund Request", "customer_tier": "GOLD", "sla_deadline_utc": "2026-01-15T18:00:00Z", "first_response_utc": null, "escalation_level": 1}, {"ticket_id": 982342, "assignment_id": 55013, "agent_id": "AGT-3387", "queue_name": "Technical-Tier1", "priority": "MEDIUM", "status": "PENDING", "category": "Product", "subcategory": "Setup Assistance", "customer_tier": "SILVER", "sla_deadline_utc": "2026-01-16T12:00:00Z", "first_response_utc": null, "escalation_level": 0}]'::jsonb,
  NULL, '{}'::jsonb, '795666942386144', 'https://example-databricks.cloud.databricks.com/jobs/123456789/runs/795666942386144', '{}'::jsonb,
  '2026-01-14T15:31:00.265+00:00'
);

SELECT setval('control.validation_history_id_seq', (SELECT MAX(id) FROM control.validation_history));
