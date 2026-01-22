-- ============================================
-- Chargement des données depuis S3 vers STAGING
-- ============================================
USE DATABASE SHOPSTREAM_DWH;

USE WAREHOUSE LOADING_WH;
USE SCHEMA STAGING;

-- 1. Chargement de STG_USERS
COPY INTO stg_users (id, email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
FROM @RAW.s3_raw_stage/postgres/users/2026-01-20/  -- AJUSTEZ LA DATE SI BESOIN
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement users terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_users;

-- 2. Chargement de STG_PRODUCTS
COPY INTO stg_products (id, merchant_id, name, description, category, price, stock_quantity, created_at, updated_at)
FROM @RAW.s3_raw_stage/postgres/products/2026-01-20/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement products terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_products;

-- 3. Chargement de STG_ORDERS
COPY INTO stg_orders (id, user_id, created_at, total_amount, status, country, payment_method)
FROM @RAW.s3_raw_stage/postgres/orders/2026-01-20/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement orders terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_orders;

-- 4. Chargement de STG_ORDER_ITEMS
COPY INTO stg_order_items (id, order_id, product_id, quantity, unit_price, line_total)
FROM @RAW.s3_raw_stage/postgres/order_items/2026-01-20/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement order_items terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_order_items;

-- 5. Chargement de STG_CRM_CONTACTS
COPY INTO stg_crm_contacts (id, email, first_name, last_name, source, campaign_id, created_at, converted, converted_at)
FROM @RAW.s3_raw_stage/postgres/crm_contacts/2026-01-20/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement crm_contacts terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_crm_contacts;

-- Récapitulatif final
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM stg_users
UNION ALL
SELECT 'products', COUNT(*) FROM stg_products
UNION ALL
SELECT 'orders', COUNT(*) FROM stg_orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM stg_order_items
UNION ALL
SELECT 'crm_contacts', COUNT(*) FROM stg_crm_contacts;