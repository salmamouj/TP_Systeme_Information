Titre: "HealthFlow Analytics - Système d’Aide à la Décision pour le Pilotage Hospitalier"

Membres du groupe: 
        -Hassan Aya 
        -Tliji Fatimazahra
        -Mouj Salma

Objectif: passer d'un système transactionnel opérationnel à une plateforme analytique scalable, auditable et BI-ready pour aider la direction de la clinique à piloter l'activité, optimiser les ressources et améliorer la prise de décision.

Technologies utilisées:
        -PostgreSQL (local)
        -Snowflake
        -AWS S3
        -Python 3.9+
        -psycopg2, faker, boto3
        -dbt Core (ou dbt Cloud)
        -Apache Airflow (Docker ou local)
        -Power BI Desktop

Instructions:
1- creation de base de donnees "healthflow_source" dans postgres
2- creation des tables "01_init_postgres.sql" dans postgres
3- generation des donnees:
        -python -m venv .venv
        -cd venv/Scripts
        -activate
        -02_python generate_data.py
4- creation du bucket dans S3 "healthflow-lake-2025"
5- creation du dossier "bronze" dans "healthflow-lake-2025"
6- creation des dossier "bronze/patients", "bronze/services", "bronze/admissions", "stocks_medicaments"
7- export des donnes de postgres vers S3 "03_python postgres_to_s3.py"
8- configuration de snowflake "04_snowflake_setup.sql"
9- chargement de "bronze S3" dans snowflake "05_loading_s3_to_snowflake.sql"
10- chargement de "staging" dans snowflake "06_loading_staging"
11- creation du dbt, dans le terminal:
       -init dbt healthflow_dbt
       -"1" pour snowflake
       -user: "votre username"
       -password: "votre password"
       -role: "ACCOUNTADMIN"
       -warehouse: "HEALTHFLOW_WH"
       -database: "HEALTHFLOW_DWH"
       -schema: "silver"
       -threads: "4"
12- dans le dossier "healthflow_dbt/models/source" creation du fichier "sources.yml"
13- creation du dossier "healthflow_dbt/models/silver" 
14- creation des fichiers sql dans "healthflow_dbt/models/silver":
        -07_silver_admissions.sql
        -08_silver_patients.sql
        -09_silver_services.sql
        -10_silver_stocks_medicaments.sql
15- chargement de "silver" dans snowflake, dans le terminal:
        -cd healthflow_dbt
        -dbt run
16- creation du SCD2 dans "healthflow_dbt/snapshots":
        -11_patients_scd2.sql
17- dans le terminal:
        -cd healthflow_dbt
        -dbt snapshot
18- dans "dbt_project.yml" ajout de:
        -model:
             gold:
               +materialized: table
               +schema: gold
19- creation des fichiers sql dans "healthflow_dbt/models/gold":
        -12_dim_patients
        -13_dim_services
        -14_dim_temps
        -15_fact_hospitalisations
20- chargement de "gold" dans snowflake, dans le terminal:
        -cd healthflow_dbt
        -dbt run --select gold.*