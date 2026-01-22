-- ============================================
-- Création du Stage externe (S3)
-- ============================================

USE DATABASE SHOPSTREAM_DWH;
USE SCHEMA RAW;
USE WAREHOUSE LOADING_WH;

-- Création du Stage
CREATE OR REPLACE STAGE s3_raw_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://shopstream-datalake-votreprenom/raw/'  -- VOTRE BUCKET ICI
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Test du Stage : liste des fichiers
LIST @s3_raw_stage/postgres/users/;

-- Si vous voyez vos fichiers CSV, c'est réussi