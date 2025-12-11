import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px

# --- Configuration ---
st.set_page_config(page_title="Chicago Transit Analytics", layout="wide")
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"

# --- Data Loading (Cached for performance) ---
@st.cache_data
def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Fetch Gold Layer Collections
    hourly_stats = list(db["gold_hourly_stats"].find({}, {"_id": 0}))
    area_stats = list(db["gold_area_stats"].find({}, {"_id": 0}))
    payment_stats = list(db["gold_payment_stats"].find({}, {"_id": 0}))
    
    return (
        pd.DataFrame(hourly_stats), 
        pd.DataFrame(area_stats), 
        pd.DataFrame(payment_stats)
    )

# --- Main App Layout ---
def main():
    st.title("🚖 Chicago Transit Analytics Dashboard")
    st.markdown("### Real-time Insights from Chicago Taxi Data (Post-2023)")
    
    # Load Data
    try:
        df_hourly, df_areas, df_payment = load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}. Is MongoDB running?")
        return

    # Row 1: Key Metrics
    c1, c2, c3 = st.columns(3)
    total_trips = df_hourly['total_trips'].sum() if not df_hourly.empty else 0
    avg_fare = df_hourly['avg_fare'].mean() if not df_hourly.empty else 0
    
    c1.metric("Total Analyzed Trips", f"{total_trips:,}")
    c2.metric("Average Fare", f"${avg_fare:.2f}")
    c3.metric("Data Source", "Chicago Data Portal (Socrata)")

    st.divider()

    # Row 2: Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🕒 Peak Traffic Hours")
        if not df_hourly.empty:
            fig_hourly = px.bar(
                df_hourly, x="hour", y="total_trips",
                title="Total Trips by Hour of Day",
                labels={"hour": "Hour (24h)", "total_trips": "Number of Trips"},
                color="total_trips",
                color_continuous_scale="Viridis"
            )
            # FIX: Using 'width="stretch"' exactly as requested by logs
            st.plotly_chart(fig_hourly, key="hourly", width="stretch")

    with col2:
        st.subheader("💳 Payment Methods")
        if not df_payment.empty:
            fig_payment = px.pie(
                df_payment, names="payment_type", values="count",
                title="Distribution of Payment Types",
                hole=0.4
            )
            # FIX: Using 'width="stretch"' exactly as requested by logs
            st.plotly_chart(fig_payment, key="payment", width="stretch")

    # Row 3: Top Areas
    st.subheader("📍 Top 10 Pickup Areas")
    if not df_areas.empty:
        fig_areas = px.bar(
            df_areas, x="pickup_community_area", y="trip_count",
            title="Most Popular Pickup Community Areas",
            labels={"pickup_community_area": "Community Area Code", "trip_count": "Trip Count"},
            color="avg_fare",
            color_continuous_scale="Magma"
        )
        # FIX: Using 'width="stretch"' exactly as requested by logs
        st.plotly_chart(fig_areas, key="areas", width="stretch")

if __name__ == "__main__":
    main()