-- ============================================
-- Création des tables de STAGING
-- ============================================
USE DATABASE SHOPSTREAM_DWH;

USE SCHEMA STAGING;

-- Table STG_USERS
CREATE OR REPLACE TABLE stg_users (
    id INT,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(3),
    plan_type VARCHAR(20),
    created_at TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_PRODUCTS
CREATE OR REPLACE TABLE stg_products (
    id INT,
    merchant_id INT,
    name VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_ORDERS
CREATE OR REPLACE TABLE stg_orders (
    id INT,
    user_id INT,
    created_at TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    country VARCHAR(3),
    payment_method VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_ORDER_ITEMS
CREATE OR REPLACE TABLE stg_order_items (
    id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_CRM_CONTACTS
CREATE OR REPLACE TABLE stg_crm_contacts (
    id INT,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    source VARCHAR(100),
    campaign_id VARCHAR(100),
    created_at TIMESTAMP,
    converted BOOLEAN,
    converted_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Vérification
SHOW TABLES;

SELECT 'Tables de STAGING créées avec succès' AS message;