{{ config(materialized='table') }}

SELECT
    ID AS admission_id,
    PATIENT_ID AS patient_id,
    SERVICE_ID AS service_id,
    DATE_ENTREE AS date_entree,
    DATE_SORTIE AS date_sortie,
    SCORE_GRAVITE AS score_gravite,
    TRIM(MOTIF) AS motif_admission,
    COUT_TOTAL AS cout_total,
    _EXTRACTED_AT AS last_extracted_at
FROM {{ source('bronze', 'STG_ADMISSIONS') }}
WHERE DATE_ENTREE IS NOT NULL
  AND (DATE_SORTIE IS NULL OR DATE_SORTIE >= DATE_ENTREE)