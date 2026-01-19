{{ config(materialized='table') }}

WITH admissions_enrichies AS (
    SELECT
        a.admission_id,
        a.patient_id,
        a.service_id,
        a.date_entree,
        a.date_sortie,
        a.score_gravite,
        a.motif_admission,
        a.cout_total,
        -- Calcul durée séjour en jours (si sortie NULL → en cours)
        COALESCE(DATEDIFF('day', a.date_entree, a.date_sortie), 
                 DATEDIFF('day', a.date_entree, CURRENT_DATE)) AS duree_sejour_jours,
        -- Clé pour dim_temps (date d'entrée)
        DATE(a.date_entree) AS date_entree_jour
    FROM {{ ref('silver_admissions') }} a
)

SELECT
    ae.admission_id AS hospitalisation_id,
    ae.patient_id,
    ae.service_id,
    ae.date_entree,
    ae.date_sortie,
    ae.duree_sejour_jours,
    ae.score_gravite,
    ae.motif_admission,
    ae.cout_total,
    1 AS nb_admissions,  -- pour compter facilement
    CURRENT_TIMESTAMP AS date_load
FROM admissions_enrichies ae
LEFT JOIN {{ ref('dim_temps') }} dt 
  ON ae.date_entree_jour = dt.date_jour