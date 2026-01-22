-- ============================================
-- Création de la Storage Integration (S3 - Snowflake)
-- ============================================
USE DATABASE SHOPSTREAM_DWH;

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::034894101137:role/snowflake-s3-integration-role1'  -- VOTRE ARN ICI
    STORAGE_ALLOWED_LOCATIONS = ('s3://shopstream-datalake-salma/raw/');  -- VOTRE BUCKET ICI

-- Récupération des informations pour AWS
DESC INTEGRATION s3_integration;