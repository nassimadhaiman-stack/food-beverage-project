import warnings
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# TITLE
# ─────────────────────────────────────────────
st.title("🏷️ Promotions Analytics")

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    session = get_active_session()
    query = """
        SELECT
            PROMOTION_ID, REGION, PRODUCT_CATEGORY, PROMOTION_TYPE,
            PROMOTION_STATUS, DISCOUNT_RATE as DISCOUNT_PERCENTAGE,
            START_DATE, END_DATE,
            NB_TRANSACTIONS_DURING_PROMO,
            TOTAL_SALES_DURING_PROMO,
            AVG_BASKET_DURING_PROMO
        FROM FOOD_BEVERAGE.ANALYTICS.PROMOTIONS_ACTIVES
    """
    return session.sql(query).to_pandas()

df = load_data()

# Prevent crash if no data
if df.empty:
    st.warning("No data available.")
    st.stop()

# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    sel_regions = st.multiselect(
        "Region",
        df["REGION"].dropna().unique(),
        default=df["REGION"].dropna().unique()
    )

    sel_categories = st.multiselect(
        "Category",
        df["PRODUCT_CATEGORY"].dropna().unique(),
        default=df["PRODUCT_CATEGORY"].dropna().unique()
    )

    sel_status = st.multiselect(
        "Status",
        df["PROMOTION_STATUS"].dropna().unique(),
        default=df["PROMOTION_STATUS"].dropna().unique()
    )

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
dff = df[
    df["REGION"].isin(sel_regions) &
    df["PRODUCT_CATEGORY"].isin(sel_categories) &
    df["PROMOTION_STATUS"].isin(sel_status)
]

if dff.empty:
    st.warning("No data after filtering.")
    st.stop()

# ─────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────
total_sales = dff["TOTAL_SALES_DURING_PROMO"].sum()
avg_discount = dff["DISCOUNT_PERCENTAGE"].mean() * 100

c1, c2 = st.columns(2)

with c1:
    st.metric("Total Sales", f"${total_sales:,.0f}")

with c2:
    st.metric("Avg Discount", f"{avg_discount:.1f}%")

# ─────────────────────────────────────────────
# CHART: SALES BY REGION
# ─────────────────────────────────────────────
st.subheader("Sales by Region")

region_sales = (
    dff.groupby("REGION")["TOTAL_SALES_DURING_PROMO"]
    .sum()
    .reset_index()
)

fig = px.bar(
    region_sales,
    x="REGION",
    y="TOTAL_SALES_DURING_PROMO",
    title="Total Sales by Region"
)


st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# TABLE
# ─────────────────────────────────────────────
st.subheader("Detailed Data")

table_df = dff.copy()

# Correct formatting
table_df["DISCOUNT_PERCENTAGE"] = (
    table_df["DISCOUNT_PERCENTAGE"] * 100
).round(1).astype(str) + "%"

st.dataframe(table_df, use_container_width=True)
 
