--USE DATABASE HEALTHFLOW_DWH;
--USE SCHEMA STAGING;  

-- Création (ou recréation) du stage avec clés AWS directement
CREATE OR REPLACE STAGE S3_BRONZE_STAGE
  URL = 's3://healthflow-lake-2025/bronze/'  -- Adapte si ton bucket/path est différent
  STORAGE_INTEGRATION = mon_integration_s3
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = AUTO)
  COMMENT = 'Stage Bronze pour HealthFlow';