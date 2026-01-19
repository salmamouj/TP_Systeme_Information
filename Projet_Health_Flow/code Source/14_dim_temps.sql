{{ config(materialized='table') }}

WITH dates AS (
    SELECT 
        SEQ4() AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))  -- 4 ans ~1461 jours
)
SELECT
    DATEADD('day', n, '2023-01-01'::DATE) AS date_jour,
    YEAR(date_jour) AS annee,
    MONTH(date_jour) AS mois,
    DAY(date_jour) AS jour,
    QUARTER(date_jour) AS trimestre,
    MONTHNAME(date_jour) AS nom_mois,
    DAYNAME(date_jour) AS nom_jour,
    CASE WHEN DAYOFWEEK(date_jour) IN (1,7) THEN 'Weekend' ELSE 'Semaine' END AS type_jour,
    CURRENT_TIMESTAMP AS date_load
FROM dates
WHERE date_jour <= '2026-12-31'