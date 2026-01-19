{{ config(materialized='table') }}

SELECT
    ID AS patient_id,
    UPPER(TRIM(NOM)) AS nom,
    UPPER(TRIM(PRENOM)) AS prenom,
    DATE_NAISSANCE AS date_naissance,
    TRIM(ADRESSE) AS adresse,
    UPPER(TRIM(MUTUELLE)) AS mutuelle,
    TELEPHONE,
    EMAIL,
    DATE_CREATION AS date_creation,
    DATE_MODIFICATION AS date_modification,
    _EXTRACTED_AT AS last_extracted_at
FROM {{ source('bronze', 'STG_PATIENTS') }}
WHERE NOM IS NOT NULL
  AND PRENOM IS NOT NULL
  AND ID IS NOT NULL