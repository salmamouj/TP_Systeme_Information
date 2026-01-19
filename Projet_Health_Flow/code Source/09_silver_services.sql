{{ config(materialized='table') }}

SELECT
    ID AS service_id,
    UPPER(TRIM(NOM_SERVICE)) AS nom_service,
    CAPACITE_LITS AS capacite_lits,
    UPPER(TRIM(RESPONSABLE)) AS responsable,
    ETAGE AS etage,
    DATE_CREATION AS date_creation,
    _EXTRACTED_AT AS last_extracted_at
FROM {{ source('bronze', 'STG_SERVICES') }}