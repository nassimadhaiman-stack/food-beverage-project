-- Analyse de l’évolution des ventes dans le temps
SELECT
    DATE_TRUNC('MONTH', TRANSACTION_DATE) AS sales_month,
    SUM(AMOUNT) AS CA,
    COUNT(TRANSACTION_ID) AS volume_ventes
FROM financial_transactions_clean
WHERE TRANSACTION_TYPE = 'SALE'
GROUP BY 1
ORDER BY 1;

-- Performance_Par_produit
SELECT
    PRODUCT_ID,
    PRODUCT_CATEGORY,
    COUNT(*)                    AS nb_avis,
    ROUND(AVG(RATING), 2)       AS note_moyenne
FROM FOOD_BEVERAGE.FB_SILVER.product_reviews_clean
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY note_moyenne DESC;

-- Performance_par_categorie
SELECT
    PRODUCT_CATEGORY,
    COUNT(*)                    AS nb_avis,
    ROUND(AVG(RATING), 2)       AS note_moyenne,
    COUNT(CASE WHEN RATING >= 4 THEN 1 END) AS avis_positifs,
    COUNT(CASE WHEN RATING <= 2 THEN 1 END) AS avis_negatifs
FROM FOOD_BEVERAGE.FB_SILVER.product_reviews_clean
GROUP BY 1
ORDER BY note_moyenne DESC;

-- Performance_par_region
SELECT
    REGION,
    COUNT(DISTINCT TRANSACTION_ID)      AS nb_transactions,
    ROUND(SUM(AMOUNT), 2)               AS ca_total,
    ROUND(AVG(AMOUNT), 2)               AS panier_moyen
FROM FOOD_BEVERAGE.FB_SILVER.financial_transactions_clean
WHERE TRANSACTION_TYPE = 'SALE'
GROUP BY 1
ORDER BY ca_total DESC;

-- Impact_des_interactions_sur_les_ventes
   -- voir si le chiffre d'affaire monte ou descend selon le fait que la satisfaction des clients monte ou descende
USE schema FOOD_BEVERAGE.FB_SILVER;
WITH interactions_stats AS (
    SELECT 
        TO_CHAR(interaction_date, 'YYYY-MM') AS year_month,
        COUNT(*) as total_interactions,
        AVG(customer_satisfaction) as avg_satisfaction
    FROM customer_service_interactions_clean
    GROUP BY 1
),
sales_stats AS (
    SELECT 
        TO_CHAR(transaction_date, 'YYYY-MM') AS year_month,
        SUM(amount) AS total_sales
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    WHERE transaction_type = 'SALE'
    GROUP BY 1
)
SELECT 
    i.year_month,
    i.avg_satisfaction,
    i.total_interactions,
    s.total_sales
FROM interactions_stats i
JOIN sales_stats s ON i.year_month = s.year_month
ORDER BY i.year_month, i.avg_satisfaction desc ;

-- Impact_des_avis_sur_les_ventes
WITH reviews_stats AS (
    SELECT
        TO_CHAR(REVIEW_DATE, 'YYYY-MM')  AS year_month,
        ROUND(AVG(RATING), 2)            AS avg_rating,
        COUNT(*)                         AS nb_avis
    FROM product_reviews_clean
    GROUP BY 1
),
sales_stats AS (
    SELECT
        TO_CHAR(TRANSACTION_DATE, 'YYYY-MM') AS year_month,
        ROUND(SUM(AMOUNT), 2)                AS total_sales
    FROM financial_transactions_clean
    WHERE TRANSACTION_TYPE = 'SALE'
    GROUP BY 1
)
SELECT
    r.year_month,
    r.avg_rating,
    r.nb_avis,
    s.total_sales
FROM reviews_stats r
JOIN sales_stats s ON r.year_month = s.year_month
ORDER BY r.year_month, r.avg_rating desc;

-- Répartition des clients par segments démographiques

-- Par région
SELECT 
    Region,
    count(*) as nb_customers,
    FROM FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;
    
-- Par gender
SELECT 
    Gender,
    count(*) as nb_customers,
    FROM FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;

-- Par marital_status
SELECT 
    MARITAL_STATUS,
    count(*) as nb_customers,
    FROM FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;

WITH customer_age_class AS (

    SELECT 

        CUSTOMER_ID,

        DATE_OF_BIRTH,

        TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') AS AGE,

        CASE

            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') < 25              THEN 'Young Adult'

            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') BETWEEN 25 AND 44 THEN 'Adult'

            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') BETWEEN 45 AND 64 THEN 'Middle Aged'

            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') >= 65             THEN 'Senior'

        END AS AGE_GROUP

    FROM FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean

),

customer_income_class AS (

    SELECT

        CUSTOMER_ID,

        CASE

            WHEN ANNUAL_INCOME BETWEEN 20038  AND 64998  THEN 'Low Income'

            WHEN ANNUAL_INCOME BETWEEN 64999  AND 109959 THEN 'Middle Income'

            WHEN ANNUAL_INCOME BETWEEN 109960 AND 154920 THEN 'Upper-Middle Income'

            WHEN ANNUAL_INCOME BETWEEN 154921 AND 199881 THEN 'High Income'

        END AS INCOME_CATEGORY

    FROM FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean

)
 
SELECT 
    a.AGE_GROUP,
    i.INCOME_CATEGORY,
    COUNT(a.CUSTOMER_ID) as nb_customers
FROM customer_age_class a
JOIN customer_income_class i ON a.CUSTOMER_ID = i.CUSTOMER_ID
GROUP BY 1, 2
ORDER BY 1, 2;    
