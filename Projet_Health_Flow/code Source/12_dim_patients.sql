{{ config(materialized='table') }}

SELECT
    patient_id,
    nom,
    prenom,
    date_naissance,
    adresse,
    mutuelle,
    telephone,
    email,
    date_creation,
    date_modification,
    dbt_scd_id AS version_id,
    dbt_valid_from,
    dbt_valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS est_version_courante,
    CURRENT_TIMESTAMP AS date_load
FROM {{ ref('patients_scd_type2') }}  -- nom exact de ton snapshot