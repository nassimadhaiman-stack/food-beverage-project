import warnings
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CONFIGURATION DE LA PAGE
# ─────────────────────────────────────────────
st.title("👥 Customer Intelligence Dashboard")

# ─────────────────────────────────────────────
# CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_customer_data():
    session = get_active_session()
    query = """
        SELECT 
            CUSTOMER_ID, REGION, GENDER, MARITAL_STATUS, 
            AGE, AGE_GROUP, ANNUAL_INCOME, INCOME_CATEGORY,
            TOTAL_TRANSACTIONS, TOTAL_SALES, AVG_BASKET, RECENCY_DAYS
        FROM FOOD_BEVERAGE.ANALYTICS.CUSTOMERS_ENRICHED
    """
    return session.sql(query).to_pandas()

df = load_customer_data()

if df.empty:
    st.warning("Aucune donnée disponible.")
    st.stop()

# ─────────────────────────────────────────────
# BARRE LATÉRALE - FILTRES
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("🎯 Segmentation")
    sel_regions = st.multiselect("Régions", df["REGION"].unique(), default=df["REGION"].unique())
    sel_income = st.multiselect("Niveau de Revenu", df["INCOME_CATEGORY"].unique(), default=df["INCOME_CATEGORY"].unique())
    sel_age = st.multiselect("Tranche d'Âge", df["AGE_GROUP"].unique(), default=df["AGE_GROUP"].unique())

# Application des filtres
dff = df[
    df["REGION"].isin(sel_regions) & 
    df["INCOME_CATEGORY"].isin(sel_income) & 
    df["AGE_GROUP"].isin(sel_age)
]

if dff.empty:
    st.warning("Aucune donnée après filtrage.")
    st.stop()

# ─────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("Total Customers", f"{len(dff):,}")
with c2:
    st.metric("Avg Annual Income", f"${dff['ANNUAL_INCOME'].mean():,.0f}")
with c3:
    st.metric("Median Age", f"{int(dff['AGE'].median())}")
with c4:
    st.metric("Avg Regional Basket", f"${dff['AVG_BASKET'].mean():,.2f}")

st.divider()

# ─────────────────────────────────────────────
# ANALYSE VISUELLE
# ─────────────────────────────────────────────

# LIGNE 1 : Démographie (Ton préféré) et Revenus

st.subheader("Profil Âge & Genre")
fig_age = px.histogram(
        dff, 
        x="AGE_GROUP", 
        color="GENDER", 
        barmode="group",
        title="Distribution Démographique",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        category_orders={"AGE_GROUP": ["Young Adult", "Adult", "Middle Aged", "Senior"]}
    )
st.plotly_chart(fig_age, use_container_width=True)

# LIGNE 2 : Classification par salaire 
st.subheader("Distribution du salaire annuel par Région et Âge")
fig_basket = px.bar(
    dff, 
    x="REGION", 
    y="ANNUAL_INCOME", 
    color="AGE_GROUP", 
    barmode="group",
    title="Analyse du salaire annuel",
    category_orders={"AGE_GROUP": ["Young Adult", "Adult", "Middle Aged", "Senior"]}
)
st.plotly_chart(fig_basket, use_container_width=True)

# LIGNE 3 : Performance par Région
st.subheader("Performance du Panier Moyen par Région et Âge")
fig_basket = px.bar(
    dff, 
    x="REGION", 
    y="AVG_BASKET", 
    color="AGE_GROUP", 
    barmode="group",
    title="Analyse du Panier Moyen (Inférence Régionale)",
    category_orders={"AGE_GROUP": ["Young Adult", "Adult", "Middle Aged", "Senior"]}
)
st.plotly_chart(fig_basket, use_container_width=True)

# ─────────────────────────────────────────────
# TABLEAU FINAL (Visible directement)
# ─────────────────────────────────────────────
st.subheader("📋 Liste Client Enrichie")
st.dataframe(dff, use_container_width=True, height=450)
