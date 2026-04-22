# AnyCompany Food & Beverage - Data-Driven Marketing Analytics

> **Projet MBA ESG 2026** · Snowflake · Streamlit · SQL Analytics
> ** Equipe : Assitan SINEYOKO & Nassima DHAIMAN

## Contexte business

AnyCompany Food & Beverage est un fabricant de produits alimentaires et de boissons présent sur le marché depuis plus de 25 ans. En 2025, l'entreprise traverse une crise : **baisse des ventes inédite**, **budget marketing réduit de 30 %**, et une **perte de 6 points de part de marché** (de 28 % à 22 %) en seulement huit mois, face à des concurrents *digital-first* s'appuyant sur des stratégies fortement pilotées par la donnée.

Le PDG a lancé une **initiative de transformation digitale** confiée à Sarah, Senior Marketing Executive. L'objectif est d'atteindre **32 % de part de marché d'ici le T4 2025** en exploitant les données existantes pour cibler les produits et segments à fort potentiel.

---

## Architecture technique

```
Amazon S3 (logbrain-datalake)
        │
        ▼
   FB_STAGE (Snowflake External Stage)
        │
        ├──► FOOD_BEVERAGE.FB_BRONZE   (données brutes)
        │
        └──► FOOD_BEVERAGE.FB_SILVER   (données nettoyées)
                    │
                    └──► FOOD_BEVERAGE.ANALYTICS  (vues analytiques & data products)


```
## Structure du projet

```
food-beverage-project/
├── sql/
│   ├── Load_data.sql            # DDL tables BRONZE + COPY INTO + parsing JSON
│   ├── clean_data.sql           # Tables SILVER nettoyées (toutes les 11 tables)
│   ├── sales_trends.sql         # Analyse temporelle des ventes
│   ├── promotion_impact.sql     # Impact des promotions sur les ventes
│   └── campaign_performance.sql # ROI et performance des campagnes
├── streamlit/
│   ├── promotion analytics.py    # Dashboard Promotions Analytics
│   ├── customer analytics.py    # Customer Intelligence Dashboard
│   └── region analytics.py      # Region Analytics Dashboard
├── ml/
│   ├── promotions actives.sql   # Vue data product - promotions enrichies
│   ├── clients enrechis.sql   # Vue data product - clients enrichis
│   └── regions enrichies.sql      # Vue data product - region enrichies
└── README.md
```

---

## ⚙️ Phase 1 - Data Preparation & Ingestion

### Étape 1 - Environnement Snowflake

```sql
CREATE DATABASE IF NOT EXISTS FOOD_BEVERAGE;
CREATE SCHEMA IF NOT EXISTS FOOD_BEVERAGE.FB_BRONZE;
CREATE SCHEMA IF NOT EXISTS FOOD_BEVERAGE.FB_SILVER;

CREATE STAGE IF NOT EXISTS FB_STAGE
    url = 's3://logbrain-datalake/datasets/food-beverage/';
```

### Étape 2 - Tables créées dans FB_BRONZE

| Table | Source | Format | 
|---|---|---|
| `customer_demographics` | `customer_demographics.csv` | CSV | 
| `customer_service_interactions` | `customer_service_interactions.csv` | CSV | 
| `financial_transactions` | `financial_transactions.csv` | CSV |
| `promotion_data` | `promotions-data.csv` | CSV |
| `marketing_campaigns` | `marketing_campaigns.csv` | CSV | 
| `product_reviews` | `product_reviews.csv` | CSV (TSV) |
| `inventory` | `inventory.json` | JSON → VARIANT | 
| `store_location` | `store_locations.json` | JSON → VARIANT |
| `logistics_and_shipping` | `logistics_and_shipping.csv` | CSV | 
| `supplier_information` | `supplier_information.csv` | CSV | 
| `employee_records` | `employee_records.csv` | CSV |

### Étape 3 - Chargement

Trois formats de fichier déclarés :

```sql
-- CSV standard
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = TRUE TRIM_SPACE = TRUE;

-- JSON (tableau d'objets)
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON' strip_outer_array = true;

-- TSV pour product_reviews (séparateur tabulation)
CREATE OR REPLACE FILE FORMAT csv_reviews
    TYPE = 'CSV' FIELD_DELIMITER = '\t' SKIP_HEADER = 1;
```

> **Point notable** : le fichier `product_reviews.csv` utilise un délimiteur tabulation et contient des colonnes supplémentaires. Le `COPY INTO` utilise une projection positionnelle `$1, $2, $3, $4, $7, $8, $9, $10, $11` avec `ON_ERROR = 'CONTINUE'`.

> **Fichiers JSON** : ingérés via tables VARIANT intermédiaires (`Json_inventory`, `Json_store_location`), puis transférés dans les tables structurées via `INSERT INTO ... SELECT v:field::TYPE`, avant suppression des tables VARIANT.

### Étape 4 - Vérifications

```sql
-- Contrôles clés sur financial_transactions
SELECT
    COUNT(DISTINCT transaction_id)  AS ids_uniques,
    MIN(transaction_date)           AS date_min,
    MAX(transaction_date)           AS date_max,
    COUNT(DISTINCT entity)          AS nb_entites,
    COUNT(DISTINCT region)          AS nb_regions,
    COUNT(CASE WHEN transaction_id IS NULL THEN 1 END)   AS ids_manquants,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS dates_manquantes
FROM financial_transactions;
```

---

### Étape 5 - Data Cleaning (schéma FB_SILVER)

Pour chaque table BRONZE, une table `*_clean` est créée dans `FB_SILVER`. Voici les règles appliquées :

#### Règles communes à toutes les tables
- `TRIM()` sur toutes les colonnes texte
- `TO_DATE()` pour harmoniser toutes les dates
- `ABS()` sur les montants, salaires et stocks (valeurs positives obligatoires)
- `QUALIFY ROW_NUMBER() = 1` pour dédupliquer par clé primaire

#### Règles spécifiques par table

**`customer_demographics_clean`**
- `REGEXP_REPLACE` pour supprimer les titres (MD, DDS, PhD, Jr., Sr., IV, III)
- `COALESCE(NULLIF(GENDER,''), 'Other')` - genre manquant → `'Other'`
- `COALESCE(NULLIF(MARITAL_STATUS,''), 'Unknown')` - statut manquant → `'Unknown'`
- `UPPER` sur REGION, `INITCAP` sur COUNTRY et CITY

**`financial_transactions_clean`**
- `UPPER` sur TRANSACTION_TYPE, PAYMENT_METHOD, ENTITY, REGION
- Déduplication par `TRANSACTION_ID` (QUALIFY)

**`promotion_data_clean`**
- Normalisation du discount : `CASE WHEN DISCOUNT_PERCENTAGE > 1 THEN / 100 ELSE ...` (certaines valeurs étaient en %, d'autres en décimal)
- Filtre `END_DATE >= START_DATE` + exclusion des régions aberrantes (`'0'`, `'1'`)

**`marketing_campaigns_clean`**
- `REPLACE(BUDGET, ' ', '')` puis `CAST AS NUMBER(15,2)` - les montants contenaient des espaces comme séparateurs de milliers
- Même traitement pour `REACH`
- Filtre `END_DATE >= START_DATE`

**`product_reviews_clean`**
- `REGEXP_REPLACE(REVIEW_TEXT, '<br\s*/?>', ' ')` - suppression des balises HTML
- `INITCAP` sur reviewer_name, review_title, product_category

**`inventory_clean`**
- `GREATEST(CURRENT_STOCK, 0)` - pas de stock négatif
- `GREATEST(REORDER_POINT, 50)` - seuil minimum à 50 unités
- `GREATEST(LEAD_TIME, 1)` - délai minimum à 1 jour

**`logistics_shipping_clean`**
- `COALESCE(NULLIF(DESTINATION_REGION,''), 'UNKNOWN')` - régions vides traitées
- `COALESCE(NULLIF(DESTINATION_COUNTRY,''), 'UNKNOWN')` - pays vides traités

**`supplier_information_clean`**
- `CASE WHEN RELIABILITY_SCORE > 1 THEN 1 WHEN < 0 THEN 0 ELSE ...` - bornes [0,1]

**`employee_records_clean`**
- `REPLACE(SALARY, ' ', '')` puis `CAST AS NUMBER(12,2)` - salaires avec espaces
- `LOWER(TRIM(EMAIL))` - normalisation des emails

---

## Phase 2 - Exploration & Analyses Business

### Partie 2.1 - Périmètre des datasets (schéma SILVER)

| Table | Périmètre métier | KPI central |
|---|---|---|
| `customer_demographics_clean` | Référentiel clients socio-démographique | `ANNUAL_INCOME` |
| `customer_service_interactions_clean` | Qualité de service & satisfaction | `CUSTOMER_SATISFACTION` |
| `financial_transactions_clean` | Flux financiers - revenus et dépenses | `AMOUNT` (ventes) |
| `promotion_data_clean` | Offres commerciales et remises | `DISCOUNT_RATE` |
| `marketing_campaigns_clean` | Performance & ROI marketing | `CONVERSION_RATE` |
| `product_reviews_clean` | Voix du client - satisfaction produit | `RATING` |
| `inventory_clean` | Niveaux de stock & réapprovisionnement | `CURRENT_STOCK` vs `REORDER_POINT` |
| `store_location_clean` | Réseau de points de vente physiques | `EMPLOYEE_COUNT`, `SQUARE_FOOTAGE` |
| `logistics_shipping_clean` | Suivi des expéditions & délais | `SHIPPING_COST`, `STATUS` |
| `supplier_information_clean` | Fiabilité & qualité fournisseurs | `RELIABILITY_SCORE` |
| `employee_records_clean` | RH - effectifs, postes, masse salariale | `ANNUAL_SALARY` |

### Partie 2.2 - Analyses exploratoires

Contrôles systématiques appliqués à chaque table :
- Comptage de lignes (`COUNT(*)`)
- Détection des doublons (`GROUP BY PK HAVING COUNT > 1`)
- Valeurs manquantes (`COUNT(CASE WHEN col IS NULL THEN 1 END)`)
- Distribution des variables catégorielles (`GROUP BY`)
- Statistiques des variables numériques (`MIN / MAX / AVG`)

---

### Phase 3 - Data Products & Analytics

#### Schéma ANALYTICS - 3 vues enrichies

##### `PROMOTIONS_ACTIVES`
Vue combinant promotions et transactions financières pour mesurer l'impact réel des promotions sur les ventes.

**Logique :** jointure temporelle entre `promotion_data_clean` et `financial_transactions_clean` sur `REGION` et `TRANSACTION_DATE BETWEEN START_DATE AND END_DATE`.

**Champs clés exposés :**
- `PROMOTION_STATUS` (Active / Expired / Upcoming)
- `PROMOTION_DURATION_DAYS`
- `NB_TRANSACTIONS_DURING_PROMO`
- `TOTAL_SALES_DURING_PROMO`
- `AVG_BASKET_DURING_PROMO`
- `SALES_PER_PROMO_DAY` (efficacité journalière de la promo)

##### `CUSTOMERS_ENRICHED`
Vue agrégeant les données démographiques clients avec les KPIs de ventes régionaux.

**Segmentations créées :**
- `AGE_GROUP` : Young Adult / Adult / Middle Aged / Senior
- `INCOME_CATEGORY` : Low / Middle / Upper-Middle / High Income

**KPIs joints depuis `financial_transactions_clean` :**
- `TOTAL_TRANSACTIONS`, `TOTAL_SALES`, `AVG_BASKET`
- `LAST_PURCHASE_DATE`, `RECENCY_DAYS`

> Note : la jointure se fait au niveau région (pas de `customer_id` dans les transactions), ce qui donne une approximation régionale des comportements d'achat.

##### `REGION_ENRICHED`
Vue consolidée à la maille région, croisant 4 sources de données :

| Dimension | Source |
|---|---|
| Démographie clients | `customer_demographics_clean` |
| Performance des ventes | `financial_transactions_clean` |
| Impact des promotions | `promotion_data_clean` |
| Efficacité des campagnes | `marketing_campaigns_clean` |

**KPIs calculés :**
- `PROMO_SALES_SHARE_PCT` - part des ventes réalisées en période promo
- `REACH_PER_BUDGET` - efficacité media des campagnes
- `DOMINANT_CAMPAIGN_TYPE` / `DOMINANT_TARGET_AUDIENCE`

---

## Dashboards Streamlit

### 1. Promotions Analytics (`promotion_analysis.py`)
**Source :** `FOOD_BEVERAGE.ANALYTICS.PROMOTIONS_ACTIVES`

- **Filtres :** Région, Catégorie produit, Statut promotion
- **KPIs :** Total Sales, Avg Discount %
- **Graphique :** Ventes totales par région (bar chart)
- **Tableau :** Détail des promotions filtrées avec discount formaté en %

### 2. Customer Intelligence Dashboard (`customer_dashboard.py`)
**Source :** `FOOD_BEVERAGE.ANALYTICS.CUSTOMERS_ENRICHED`

- **Filtres :** Région, Niveau de revenu, Tranche d'âge
- **KPIs :** Total Customers, Avg Annual Income, Median Age, Avg Regional Basket
- **Graphique 1 :** Distribution démographique Âge × Genre (histogram groupé)
- **Graphique 2 :** Salaire annuel par Région × Âge (bar groupé)
- **Graphique 3 :** Panier moyen par Région × Âge (bar groupé)
- **Tableau :** Liste client enrichie complète

### 3. Region Analytics Dashboard (`region_dashboard.py`)
**Source :** `FOOD_BEVERAGE.ANALYTICS.REGION_ENRICHED`

- **Filtres :** Région, Type de campagne dominant
- **KPIs :** Total Sales, Promo Sales, Avg Conversion Rate, Total Budget
- **Graphique 1 :** Ventes totales par région (bar chart)
- **Graphique 2 :** Budget campagne vs ventes promo × reach (scatter bubble)
- **Graphique 3 :** Taux de conversion par type de campagne (bar chart)
- **Graphique 4 :** Distribution de la part promo dans les ventes par région (box plot)
- **Tableau :** Données régionales détaillées avec KPIs formatés

---

## Points techniques notables

| Défi | Solution retenue |
|---|---|
| `product_reviews.csv` avec délimiteur TSV et colonnes extras | Format `csv_reviews` (FIELD_DELIMITER=`\t`) + projection positionnelle `$1,$2...` |
| Fichiers JSON = tableaux d'objets | `strip_outer_array=true` + tables VARIANT intermédiaires |
| Doublons dans `customer_demographics` | `QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID) = 1` |
| Montants/salaires avec espaces comme séparateurs | `REPLACE(col, ' ', '')` avant `CAST AS NUMBER` |
| Discount parfois en % (ex: 15) parfois en décimal (0.15) | `CASE WHEN > 1 THEN /100 ELSE col END` |
| Régions vides dans `logistics_and_shipping` | `COALESCE(NULLIF(...,''), 'UNKNOWN')` |
| Jointure promo ↔ transactions sans clé directe | Jointure spatiale et temporelle sur `REGION` + `BETWEEN START_DATE AND END_DATE` |

---

