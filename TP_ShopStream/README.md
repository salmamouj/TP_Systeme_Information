Titre: "TP Cloud Pipeline moderne PostgreSQL → S3 → Snowflake → dbt → Airflow → BI"

Membres du groupe: 
        -Mouj Salma
        -Tliji Fatimazahra
        -Hassan Aya 
        
Objectif: L'objectif principal de ce TP est de concevoir et de déployer un pipeline de données complet et automatisé, de l'extraction à la visualisation, en s'appuyant sur une architecture de type Modern Data Stack.

Technologies utilisées:
        - Postgres
        - pgAdmin 4
        - Amazon S3 (AWS)
        - Snowflake
        - dbt
        - Apache Airflow
        - Python (faker, psycopg2, pandas, boto3)
        - AWS CLI
        - Power BI

Instructions:
1- creation de la base de donnees "shopstream" dans postgres
2- dans postgres execution du code sql:
        - 01_oltp_postgres.sql
3- execution du code python:
        - python 02_generate_data.py
4- creation d'un bucket "shopstream-datalake-salma" dans S3
5- creation des dossiers dans S3:
        - shopstream-datalake-salma/
          --raw/
              -- postgres/
              |   |-- users/
              |   |-- products/
              |   |-- orders/
              |   |-- order_items/
              |-- events/
              |-- crm/
5- creation d'un utilisateur IAM avec acces S3
6- configuration de AWS CLI:
        - telechargement de AWS CLI
        - dans CMD: aws configure
7- chargement des donnees de postgres vers S3
        - python 03_export_to_s3.py
8- execution des codes sql dans Snowflake:
        - 04_dwh_snowflake.sql
        - 05_virtual_warehouses.sql
9- creation d'un role dans AWS
10- creation d'une Storage Integration dans Snowflake:
        - 06_storage_integration.sql
11- configuration de la relation de confiance dans AWS:
        - {
          "Version": "2012-10-17",
          "Statement": [
             {
             "Effect": "Allow",
             "Principal": {
                 "AWS": "Votre aws"
             },
             "Action": "sts:AssumeRole",
             "Condition": {
                 "StringEquals": {
                 "sts:ExternalId": "Votre external id"
                 }
             }
             }
         ]
         }
12- execution des codes sql dans Snowflake:
        - 07_stage_externe.sql
        - 08_create_staging.sql
        - 09_s3_to_snowflake.sql
13- creation + configuration de dbt "shopstream_dbt":
        - dbt init shopstream_dbt
        -# Configuration dbt pour Snowflake
         # Fichier : ~/. dbt/profiles.yml
            shopstream_dbt:
            target: dev
            outputs:
                dev:
                type: snowflake
                account: abc12345.jehysq  # VOTRE account Snowflake (sans https://)
                user: VOTRE_USERNAME_SNOWFLAKE
                password: VOTRE_MOT_DE_PASSE_SNOWFLAKE
                role: ACCOUNTADMIN
                database: SHOPSTREAM_DWH
                warehouse: TRANSFORM_WH
                schema: CORE
                threads: 4
                client_session_keep_alive: False
14- creation des dossiers "staging, core, marts" dans "shopstream_dbt/models/"
15- creation des fichiers:
        - models/staging/_staging__sources.yml
        - models/staging/stg_orders.sql
        - models/core/dimensions/dim_customers.sql
        - models/core/dimensions/dim_products.sql
        - models/core/facts/fact_orders.sql
        - models/marts/mart_sales_overview.sql
16- execution du dbt: /shopstream_dbt/dbt run
17- creation et execution du fichier:
        - models/marts/mart_product_performance.sql
        - dbt run --select mart_product_performance
18- creation et execution du fichier:
        - models/marts/mart_customer_ltv.sql
        - dbt run --select mart_customer_ltv
19- configuration des connexions Airflow
20- creation et activation du dag:
        - ShopStreamTP/airflow/dags/shopstream_daily_pipeline.py
