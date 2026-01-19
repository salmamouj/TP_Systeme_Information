-- Étape 1: Créer la base de données
CREATE DATABASE IF NOT EXISTS HEALTHFLOW_DWH
    COMMENT = 'Data Warehouse HealthFlow Analytics';

USE DATABASE HEALTHFLOW_DWH;

-- Étape 2: Créer les schémas
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Zone de chargement brut depuis S3';

CREATE SCHEMA IF NOT EXISTS CORE
    COMMENT = 'Données nettoyées et historisées';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Modèles métier pour BI';

-- Étape 3: Créer le Warehouse
CREATE WAREHOUSE IF NOT EXISTS HEALTHFLOW_WH
    WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse HealthFlow';

USE WAREHOUSE HEALTHFLOW_WH;

-- Étape 4: Créer les tables de staging
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE STG_PATIENTS (
    id INTEGER,
    nom VARCHAR(100),
    prenom VARCHAR(100),
    date_naissance DATE,
    adresse TEXT,
    mutuelle VARCHAR(100),
    telephone VARCHAR(20),
    email VARCHAR(150),
    date_creation TIMESTAMP,
    date_modification TIMESTAMP,
    _extracted_at TIMESTAMP,
    _source_system VARCHAR(50),
    _extraction_batch_id VARCHAR(50)
);

CREATE OR REPLACE TABLE STG_SERVICES (
    id INTEGER,
    nom_service VARCHAR(100),
    capacite_lits INTEGER,
    responsable VARCHAR(100),
    etage INTEGER,
    date_creation TIMESTAMP,
    _extracted_at TIMESTAMP,
    _source_system VARCHAR(50),
    _extraction_batch_id VARCHAR(50)
);

CREATE OR REPLACE TABLE STG_ADMISSIONS (
    id INTEGER,
    patient_id INTEGER,
    service_id INTEGER,
    date_entree TIMESTAMP,
    date_sortie TIMESTAMP,
    score_gravite INTEGER,
    motif TEXT,
    cout_total DECIMAL(10,2),
    _extracted_at TIMESTAMP,
    _source_system VARCHAR(50),
    _extraction_batch_id VARCHAR(50)
);

CREATE OR REPLACE TABLE STG_STOCKS_MEDICAMENTS (
    id INTEGER,
    nom_medicament VARCHAR(200),
    quantite INTEGER,
    seuil_alerte INTEGER,
    prix_unitaire DECIMAL(10,2),
    date_peremption DATE,
    fournisseur VARCHAR(150),
    date_derniere_commande DATE,
    date_creation TIMESTAMP,
    _extracted_at TIMESTAMP,
    _source_system VARCHAR(50),
    _extraction_batch_id VARCHAR(50)
);

-- Vérification
SHOW TABLES IN SCHEMA STAGING;

SELECT 'Configuration Snowflake terminée ✅' AS status;