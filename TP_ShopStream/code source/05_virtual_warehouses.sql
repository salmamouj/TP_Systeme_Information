-- ============================================
-- Création des Virtual Warehouses
-- ============================================
USE DATABASE SHOPSTREAM_DWH;

-- Warehouse pour les chargements
CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse pour charger les données depuis S3';

-- Warehouse pour les transformations dbt
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse pour les transformations dbt';

-- Warehouse pour la BI
CREATE WAREHOUSE IF NOT EXISTS BI_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse pour les requêtes BI';

-- Vérification
SHOW WAREHOUSES;

SELECT 'Warehouses créés avec succès' AS message;