{{ config(materialized='table') }}

SELECT
    service_id,
    nom_service,
    capacite_lits,
    responsable,
    etage,
    CURRENT_TIMESTAMP AS date_load
FROM {{ ref('silver_services') }}