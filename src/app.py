import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

# --- Dashboard Configuration ---
st.set_page_config(page_title="Chicago Transit Analytics", layout="wide")

# --- Database Connection ---
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["chicago_transit"]

def load_data():
    """
    Fetches aggregated data from the MongoDB Gold Layer.
    """
    # 1. Hourly Stats
    hourly_data = list(db["gold_hourly_stats"].find({}, {"_id": 0}))
    df_hourly = pd.DataFrame(hourly_data)
    
    # 2. Payment Stats
    payment_data = list(db["gold_payment_stats"].find({}, {"_id": 0}))
    df_payment = pd.DataFrame(payment_data)

    # 3. Area Stats (The Missing Chart!)
    area_data = list(db["gold_area_stats"].find({}, {"_id": 0}))
    df_area = pd.DataFrame(area_data)
    
    return df_hourly, df_payment, df_area

# Load data
df_hourly, df_payment, df_area = load_data()

# ==============================================================================
# SIDEBAR
# ==============================================================================
with st.sidebar:
    st.header(" Dashboard Controls")
    st.markdown("This dashboard connects directly to the **Gold Layer** of the Medallion Pipeline.")
    st.code("Pipeline Status: Active \nSource: MongoDB Replica", language="text")
    st.divider()
    st.write("Developed by **Komal & Charitha**")

# ==============================================================================
# SECTION 1: Executive KPI Board
# ==============================================================================
st.title(" Chicago Taxi Analytics Dashboard")
st.markdown("### Real-time Insights from Distributed Pipeline")

# Calculate KPIs
total_trips = df_hourly["trip_count"].sum() if not df_hourly.empty else 0
avg_fare = df_hourly["avg_fare"].mean() if not df_hourly.empty else 0

if not df_hourly.empty:
    busiest_hour_row = df_hourly.loc[df_hourly["trip_count"].idxmax()]
    busiest_hour = f"{int(busiest_hour_row['hour'])}:00"
else:
    busiest_hour = "N/A"

col1, col2, col3 = st.columns(3)
col1.metric("Total Trips Analyzed", f"{total_trips:,.0f}", delta="Pipeline Live")
col2.metric("Average Fare", f"${avg_fare:.2f}", delta="USD")
col3.metric("Peak Traffic Hour", busiest_hour, delta="High Demand")

st.divider()

# ==============================================================================
# SECTION 2: Data Visualizations
# ==============================================================================

# Row 1: Hourly Trends
st.subheader(" Traffic Patterns by Hour")
if not df_hourly.empty:
    fig_hourly = px.line(
        df_hourly, 
        x="hour", 
        y="trip_count", 
        markers=True,
        title="Trip Volume over 24 Hours",
        labels={"hour": "Hour of Day", "trip_count": "Number of Trips"}
    )
    st.plotly_chart(fig_hourly, use_container_width=True)

# Row 2: Payment & Areas
col_left, col_right = st.columns(2)

with col_left:
    st.subheader(" Payment Method Distribution")
    if not df_payment.empty:
        fig_payment = px.bar(
            df_payment, 
            x="payment_type", 
            y="count", 
            color="payment_type",
            title="Preferred Payment Methods"
        )
        st.plotly_chart(fig_payment, use_container_width=True)

with col_right:
    # THE NEW VISUALIZATION
    st.subheader(" Top 10 Pickup Areas")
    if not df_area.empty:
        fig_area = px.bar(
            df_area, 
            x="trip_count", 
            y="pickup_community_area", 
            orientation='h', # Horizontal bar chart
            title="Busiest Community Areas",
            labels={"trip_count": "Total Trips", "pickup_community_area": "Community Area ID"}
        )
        # Sort so the biggest bar is on top
        fig_area.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_area, use_container_width=True)

# ==============================================================================
# SECTION 3: Export
# ==============================================================================
st.divider()
st.subheader(" Export Data")
st.write("Download the aggregated Gold layer data for offline analysis.")

if not df_hourly.empty:
    csv = df_hourly.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Hourly Data (CSV)",
        data=csv,
        file_name='gold_chicago_taxi_data.csv',
        mime='text/csv',
    )