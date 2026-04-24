import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Telecom Complaints Dashboard",
    page_icon="📊",
    layout="wide"
)

BASE_PATH = "data_lake/gold"

# ---------------- LOAD DATA ----------------
@st.cache_data(ttl=300)
def load_data():
    try:
        state = pd.read_parquet(f"{BASE_PATH}/state_kpi")
        issue = pd.read_parquet(f"{BASE_PATH}/issue_kpi")
        city = pd.read_parquet(f"{BASE_PATH}/city_kpi")
        trend = pd.read_parquet(f"{BASE_PATH}/trend_kpi")
        return state, issue, city, trend
    except Exception as e:
        st.error(f"⚠️ Run Gold Layer First!\n\nError: {e}")
        st.stop()

state_df, issue_df, city_df, trend_df = load_data()

# ---------------- TITLE ----------------
st.title("📊 Telecom Complaints Dashboard")
st.divider()

# ---------------- KPI ----------------
k1, k2, k3 = st.columns(3)

total_complaints = int(state_df["total_complaints"].sum())
total_states = state_df["state"].nunique()

top_issue = issue_df.loc[
    issue_df["total_complaints"].idxmax(), "issue_type"
]

k1.metric("Total Complaints", total_complaints)
k2.metric("States", total_states)
k3.metric("Top Issue", top_issue)

# ---------------- ROW 1 ----------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("📍 Complaints by State")

    fig = px.bar(
        state_df.nlargest(10, "total_complaints"),
        x="total_complaints",
        y="state",
        orientation="h"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("⚠️ Issue Distribution")

    fig = px.pie(
        issue_df,
        values="total_complaints",
        names="issue_type",
        hole=0.4
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------- ROW 2 ----------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("🏙️ Top Cities")

    fig = px.bar(
        city_df.nlargest(10, "total_complaints"),
        x="city",
        y="total_complaints"
    )
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("📈 Daily Trend")

    trend_df["date"] = pd.to_datetime(trend_df["date"])

    fig = px.line(
        trend_df,
        x="date",
        y="daily_complaints"
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------- FOOTER ----------------
st.markdown("---")
st.caption("📊 Telecom Data Platform | Gold Layer Connected")