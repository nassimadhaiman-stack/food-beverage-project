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

-- Répartition des clients par segments démographiques
-- Par région
SELECT 
    Region,
    count(*) as nb_customers,
    FROM customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;
    
-- Par gender
SELECT 
    Gender,
    count(*) as nb_customers,
    FROM customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;

-- Par marital_status
SELECT 
    MARITAL_STATUS,
    count(*) as nb_customers,
    FROM customer_demographics_clean
    GROUP BY 1
    ORDER BY 2;
