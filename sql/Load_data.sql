-- ============================================================
-- Load Data dans FB_BRONZE
-- ============================================================

-- ------------------------------------------------------------
-- ÉTAPE 1 : Création de la base de données et des schémas
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS FOOD_BEVERAGE;
CREATE SCHEMA IF NOT EXISTS FOOD_BEVERAGE.FB_BRONZE;
CREATE SCHEMA IF NOT EXISTS FOOD_BEVERAGE.FB_SILVER;

USE DATABASE FOOD_BEVERAGE;
USE SCHEMA FB_BRONZE;

-- ------------------------------------------------------------
-- ÉTAPE 2 : Création du stage S3
-- ------------------------------------------------------------
CREATE STAGE IF NOT EXISTS FB_STAGE
    url = 's3://logbrain-datalake/datasets/food-beverage/';

LIST @FB_STAGE;

-- ------------------------------------------------------------
-- ÉTAPE 3 : Création des tables
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE Customer_demographics(
    customer_id     INTEGER PRIMARY KEY,
    Name            STRING,
    Date_of_birth   DATE,
    Gender          STRING,
    Region          STRING,
    Country         STRING,
    City            STRING,
    Marital_Status  STRING,
    Annual_income   BIGINT
);

CREATE OR REPLACE TABLE Customer_service_interactions(
    Interaction_id      STRING PRIMARY KEY,
    Interaction_date    DATE,
    Interaction_type    STRING,
    Issue_category      STRING,
    Description         STRING,
    Duration_minutes    INTEGER,
    Resolution_Status   STRING,
    Follow_up_required  STRING,
    Customer_Satisfaction INTEGER
);

CREATE OR REPLACE TABLE Financial_transactions(
    Transaction_id      STRING PRIMARY KEY,
    Transaction_date    DATE,
    Transaction_type    STRING,
    Amount              FLOAT,
    Payment_method      STRING,
    Entity              STRING,
    Region              STRING,
    Account_code        STRING
);

CREATE OR REPLACE TABLE Promotion_data(
    Promotion_id        STRING PRIMARY KEY,
    Product_category    STRING,
    Promotion_type      STRING,
    Discount_Percentage FLOAT,
    Start_date          DATE,
    End_date            DATE,
    Region              STRING
);

CREATE OR REPLACE TABLE Marketing_campaigns(
    Campaign_id         STRING PRIMARY KEY,
    Campaign_name       STRING,
    Campaign_type       STRING,
    Product_category    STRING,
    Target_Audience     STRING,
    Start_date          DATE,
    End_date            DATE,
    Region              STRING,
    Budget              FLOAT,
    Reach               BIGINT,
    Conversion_rate     FLOAT
);

CREATE OR REPLACE TABLE Inventory(
    Product_id          STRING PRIMARY KEY,
    Product_Category    STRING,
    Region              STRING,
    Country             STRING,
    Warehouse           STRING,
    Current_stock       INTEGER,
    Reorder_point       INTEGER,
    Lead_time           INTEGER,
    Last_restock_date   DATE
);

CREATE OR REPLACE TABLE Product_reviews(
    Review_id       INTEGER PRIMARY KEY,
    Product_id      STRING REFERENCES Inventory(Product_id),
    Reviewer_id     STRING,
    Reviewer_name   STRING,
    Rating          INTEGER,
    Review_date     DATE,
    Review_title    STRING,
    Review_text     TEXT,
    Product_Category STRING
);

CREATE OR REPLACE TABLE Store_location(
    Store_id        STRING PRIMARY KEY,
    Store_name      STRING,
    store_type      STRING,
    Region          STRING,
    Country         STRING,
    City            STRING,
    Address         STRING,
    Postal_code     STRING,
    Square_footage  FLOAT,
    Employee_Count  INTEGER
);

CREATE OR REPLACE TABLE Logistics_and_shipping(
    Shipment_id             STRING PRIMARY KEY,
    Order_id                BIGINT,
    Ship_date               DATE,
    Estimated_delivery      DATE,
    Shipping_method         STRING,
    Status                  STRING,
    Shipping_cost           FLOAT,
    Destination_region      STRING,
    Destination_country     STRING,
    Carrier                 STRING
);

CREATE OR REPLACE TABLE Supplier_information(
    Supplier_id         STRING PRIMARY KEY,
    Supplier_name       STRING,
    Product_Category    STRING,
    Region              STRING,
    Country             STRING,
    City                STRING,
    Lead_time           INTEGER,
    Reliability_score   FLOAT,
    Quality_rating      VARCHAR(1)
);

CREATE OR REPLACE TABLE Employee_records(
    Employee_id     STRING PRIMARY KEY,
    Name            STRING,
    Date_of_birth   DATE,
    Hire_date       DATE,
    Department      STRING,
    Job_title       STRING,
    Salary          FLOAT,
    Region          STRING,
    Country         STRING,
    Email           STRING
);

-- Tables temporaires pour les fichiers JSON
CREATE OR REPLACE TABLE Json_inventory       (v VARIANT);
CREATE OR REPLACE TABLE Json_store_location  (v VARIANT);

-- ------------------------------------------------------------
-- ÉTAPE 4 : Création des formats de fichiers
-- ------------------------------------------------------------

-- Format CSV standard
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE                         = 'CSV'
    field_delimiter              = ','
    record_delimiter             = '\n'
    skip_header                  = 1
    field_optionally_enclosed_by = '\042'
    null_if                      = ('')
    TRIM_SPACE                   = TRUE
    EMPTY_FIELD_AS_NULL          = TRUE;

-- Format JSON
CREATE OR REPLACE FILE FORMAT json_format
    TYPE              = 'JSON'
    strip_outer_array = true;

-- Format CSV avec tabulation (pour product_reviews)
CREATE OR REPLACE FILE FORMAT csv_reviews
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = '\t'
    SKIP_HEADER                  = 1
    field_optionally_enclosed_by = '"'
    NULL_IF                      = ('')
    TRIM_SPACE                   = TRUE
    EMPTY_FIELD_AS_NULL          = TRUE;

-- ------------------------------------------------------------
-- ÉTAPE 5 : Chargement des fichiers CSV
-- ------------------------------------------------------------
COPY INTO Customer_demographics
    FROM @FB_STAGE/customer_demographics.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO Customer_service_interactions
    FROM @FB_STAGE/customer_service_interactions.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO employee_records
    FROM @FB_STAGE/employee_records.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO financial_transactions
    FROM @FB_STAGE/financial_transactions.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO logistics_and_shipping
    FROM @FB_STAGE/logistics_and_shipping.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO marketing_campaigns
    FROM @FB_STAGE/marketing_campaigns.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO promotion_data
    FROM @FB_STAGE/promotions-data.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

COPY INTO supplier_information
    FROM @FB_STAGE/supplier_information.csv
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

-- Chargement product_reviews (avec mapping colonnes)
COPY INTO product_reviews (review_id, product_id, reviewer_id, reviewer_name, rating, review_date, review_title, review_text, product_category)
    FROM (
        SELECT $1, $2, $3, $4, $7, $8, $9, $10, $11
        FROM @FB_STAGE/product_reviews.csv
    )
    FILE_FORMAT = (FORMAT_NAME = 'csv_reviews')
    ON_ERROR   = 'CONTINUE'
    FORCE      = TRUE;

-- Chargement des fichiers JSON
COPY INTO Json_inventory
    FROM @FB_STAGE/inventory.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');

COPY INTO Json_store_location
    FROM @FB_STAGE/store_locations.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');

-- ------------------------------------------------------------
-- ÉTAPE 6 : Insertion depuis les tables JSON temporaires
-- ------------------------------------------------------------
INSERT INTO Inventory (Product_id, Product_Category, Region, Country, Warehouse, Current_stock, Reorder_point, Lead_time, Last_restock_date)
SELECT
    v:product_id::STRING,
    v:product_category::STRING,
    v:region::STRING,
    v:country::STRING,
    v:warehouse::STRING,
    v:current_stock::INTEGER,
    v:reorder_point::INTEGER,
    v:lead_time::INTEGER,
    v:last_restock_date::DATE
FROM Json_Inventory;

INSERT INTO Store_location (Store_id, Store_name, store_type, Region, Country, City, Address, Postal_code, Square_footage, Employee_Count)
SELECT
    v:store_id::STRING,
    v:store_name::STRING,
    v:store_type::STRING,
    v:region::STRING,
    v:country::STRING,
    v:city::STRING,
    v:address::STRING,
    v:postal_code::STRING,
    v:square_footage::FLOAT,
    v:employee_count::INTEGER
FROM Json_Store_location;

-- Suppression des tables JSON temporaires
DROP TABLE JSON_INVENTORY;
DROP TABLE JSON_STORE_LOCATION;
