import warnings
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

warnings.filterwarnings("ignore")

st.title("📊 Region Analytics Dashboard")

# ─────────────────────────────────────────────
# LOAD DATA (FROM SNOWFLAKE)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    session = get_active_session()
    query = """
        SELECT *
        FROM FOOD_BEVERAGE.ANALYTICS.Region_enriched;   
    """
    return session.sql(query).to_pandas()

df = load_data()

# Safety
if df.empty:
    st.warning("No data available.")
    st.stop()

# Drop useless column if exists
if "Unnamed: 0" in df.columns:
    df = df.drop(columns=["Unnamed: 0"])

# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    regions = st.multiselect(
        "Region",
        df["REGION"].dropna().unique(),
        default=df["REGION"].dropna().unique()
    )

    campaign_type = st.multiselect(
        "Campaign Type",
        df["DOMINANT_CAMPAIGN_TYPE"].dropna().unique(),
        default=df["DOMINANT_CAMPAIGN_TYPE"].dropna().unique()
    )

# Apply filters
dff = df[
    df["REGION"].isin(regions) &
    df["DOMINANT_CAMPAIGN_TYPE"].isin(campaign_type)
]

if dff.empty:
    st.warning("No data after filtering.")
    st.stop()

# ─────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────
total_sales = dff["TOTAL_SALES"].sum()
promo_sales = dff["TOTAL_SALES_DURING_PROMO"].sum()
avg_conversion = dff["AVG_CONVERSION_RATE"].mean() * 100
total_budget = dff["TOTAL_CAMPAIGN_BUDGET"].sum()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Sales", f"${total_sales:,.0f}")
c2.metric("Promo Sales", f"${promo_sales:,.0f}")
c3.metric("Avg Conversion", f"{avg_conversion:.2f}%")
c4.metric("Total Budget", f"${total_budget:,.0f}")

# ─────────────────────────────────────────────
# CHART 1: SALES BY REGION
# ─────────────────────────────────────────────
st.subheader("💰 Sales by Region")

region_sales = (
    dff.groupby("REGION")["TOTAL_SALES"]
    .sum()
    .reset_index()
)

fig1 = px.bar(region_sales, x="REGION", y="TOTAL_SALES")
st.plotly_chart(fig1, use_container_width=True)

# ─────────────────────────────────────────────
# CHART 2: CAMPAIGN PERFORMANCE
# ─────────────────────────────────────────────
st.subheader("📈 Campaign Efficiency")

fig2 = px.scatter(
    dff,
    x="TOTAL_CAMPAIGN_BUDGET",
    y="TOTAL_SALES_DURING_PROMO",
    size="TOTAL_REACH",
    color="DOMINANT_CAMPAIGN_TYPE",
    hover_data=["REGION"]
)

st.plotly_chart(fig2, use_container_width=True)

# ─────────────────────────────────────────────
# CHART 3: CONVERSION RATE BY CAMPAIGN
# ─────────────────────────────────────────────
st.subheader("🎯 Conversion Rate by Campaign Type")

conv = (
    dff.groupby("DOMINANT_CAMPAIGN_TYPE")["AVG_CONVERSION_RATE"]
    .mean()
    .reset_index()
)

fig3 = px.bar(
    conv,
    x="DOMINANT_CAMPAIGN_TYPE",
    y="AVG_CONVERSION_RATE"
)

st.plotly_chart(fig3, use_container_width=True)

# ─────────────────────────────────────────────
# CHART 4: PROMO IMPACT
# ─────────────────────────────────────────────
st.subheader("🏷️ Promotion Impact")

fig4 = px.box(
    dff,
    x="REGION",
    y="PROMO_SALES_SHARE_PCT"
)

st.plotly_chart(fig4, use_container_width=True)

# ─────────────────────────────────────────────
# TABLE
# ─────────────────────────────────────────────
st.subheader("📋 Detailed Data")

table_df = dff.copy()

# Format percentages
table_df["AVG_CONVERSION_RATE"] = (table_df["AVG_CONVERSION_RATE"] * 100).round(2).astype(str) + "%"
table_df["PROMO_SALES_SHARE_PCT"] = (table_df["PROMO_SALES_SHARE_PCT"] * 100).round(2).astype(str) + "%"

st.dataframe(table_df, use_container_width=True)
 
