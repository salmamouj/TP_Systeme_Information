-- ============================================
-- Creation et configuration initiale Snowflake pour ShopStream
-- ============================================

--Creation de la base de données
CREATE DATABASE SHOPSTREAM_DWH;

-- Utilisation de la base de données
USE DATABASE SHOPSTREAM_DWH;

-- Création des schémas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- Vérification
SHOW SCHEMAS;

-- Message de confirmation
SELECT 'Configuration initiale terminée' AS message;