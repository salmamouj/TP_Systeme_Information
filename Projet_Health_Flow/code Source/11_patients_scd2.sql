{% snapshot patients_scd_type2 %}

{{ 
    config(
        target_schema='silver',
        unique_key='patient_id',
        strategy='check',
        check_cols=['adresse', 'mutuelle'],
        invalidate_hard_deletes=True
    ) 
}}

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
    last_extracted_at 
FROM {{ ref('silver_patients') }}

{% endsnapshot %}