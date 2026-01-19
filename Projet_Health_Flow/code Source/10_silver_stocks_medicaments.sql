{{ config(materialized='table') }}

SELECT
    ID AS stock_id,
    UPPER(TRIM(NOM_MEDICAMENT)) AS nom_medicament,
    QUANTITE AS quantite_stock,
    SEUIL_ALERTE AS seuil_alerte,
    PRIX_UNITAIRE AS prix_unitaire,
    DATE_PEREMPTION AS date_peremption,
    UPPER(TRIM(FOURNISSEUR)) AS fournisseur,
    DATE_DERNIERE_COMMANDE AS date_derniere_commande,
    DATE_CREATION AS date_creation,
    CASE WHEN QUANTITE < SEUIL_ALERTE THEN TRUE ELSE FALSE END AS alerte_stock_faible,
    _EXTRACTED_AT AS last_extracted_at
FROM {{ source('bronze', 'STG_STOCKS_MEDICAMENTS') }}
WHERE QUANTITE >= 0