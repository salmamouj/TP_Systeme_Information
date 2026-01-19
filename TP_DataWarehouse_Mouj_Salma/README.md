Titre: "TP : Data Warehouse Moderne"

Objectif: L'objectif de ce TP est de concevoir un Data Warehouse moderne via un pipeline de données de bout-en-bout. On apprendrai à transformer des données brutes en insights exploitables en utilisant PySpark et Delta Lake à travers une architecture Lakehouse et des modèles de données avancés (SCD Type 2, Data Vault 2.0).

Technologies utilisées:
        -Postgres
        -Python3 avec venv et packages pyspark, delta-spark, psycopg2-binary
        -postgresql-42.7.1.jar(le deplacer dans C:\TP_DataWarehouse\drivers)
        -Java 17 configuré (JAVA_HOME)

Instructions:
1- creation de la base de donnees dans postgres:
        -CREATE DATABASE retailpro_dwh;
2- execution des codes sql dans postgres:
        -01_create_tables_sources.sql
        -02_create_dim_client_scd2.sql
3- activation de l'environnement Python:
        -python -m venv venv
        -venv\Scripts\activate
4- execution du script:
        -python 03_load_scd_type2.py 
5- execution du code sql dans postgres:
        -04_create_data_vault.sql
6- creation des dossier:
        -mkdir C:\lakehouse C:\lakehouse\bronze C:\lakehouse\silver C:\lakehouse\gold
7- execution des codes python:
        -python 05_bronze_ingestion.py
        -python 06_verify_bronze.py
        -python 07_silver_transformation.py
        -python 08_gold_aggregation.py
        -python 09_generer_rapport