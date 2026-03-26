import streamlit as st
import pandas as pd
import altair as alt
from cassandra.cluster import Cluster

st.set_page_config(
    page_title="Wikipedia Analytics Dashboard",
    layout="wide"
)

cluster = Cluster(["172.27.109.206"], port=9042)
session = cluster.connect("wiki")


# ------------------------
# Helper
# ------------------------

def fetch_table(table):
    rows = session.execute(f"SELECT * FROM {table} LIMIT 200")
    return pd.DataFrame(rows)


st.title("Wikipedia Live Analytics Dashboard")


# =========================
# Metrics
# =========================

wiki_counts = fetch_table("analytics_wiki_counts")
user_activity = fetch_table("analytics_user_activity")

total_events = wiki_counts["event_count"].sum() if not wiki_counts.empty else 0
total_users = len(user_activity)

c1, c2, c3 = st.columns(3)

c1.metric("Total Events", total_events)
c2.metric("Active Users", total_users)
c3.metric("Wikis", len(wiki_counts))


st.divider()


# =========================
# Wiki distribution chart
# =========================

st.subheader("Events per Wiki")

if not wiki_counts.empty:

    chart = alt.Chart(wiki_counts).mark_bar().encode(
        x="wiki",
        y="event_count"
    )

    st.altair_chart(chart, use_container_width=True)

else:
    st.write("No data")


# =========================
# Trending pages
# =========================

st.subheader("Trending Pages")

trending = fetch_table("analytics_trending")

if not trending.empty:

    chart = alt.Chart(trending).mark_bar().encode(
        x="title",
        y="edit_count"
    )

    st.altair_chart(chart, use_container_width=True)

    st.dataframe(trending, use_container_width=True)

else:
    st.write("No trending pages")


# =========================
# Edit wars
# =========================

st.subheader("Edit War Detection")

wars = fetch_table("analytics_edit_wars")

if not wars.empty:

    chart = alt.Chart(wars).mark_circle(size=100).encode(
        x="unique_users",
        y="total_edits",
        tooltip=["title"]
    )

    st.altair_chart(chart, use_container_width=True)

    st.dataframe(wars, use_container_width=True)

else:
    st.write("No edit wars")


# =========================
# Anomalies
# =========================

st.subheader("Anomaly Detection")

anoms = fetch_table("analytics_anomalies")

if not anoms.empty:

    chart = alt.Chart(anoms).mark_circle(size=80).encode(
        x="z_score",
        y="wiki",
        tooltip=["title", "user"]
    )

    st.altair_chart(chart, use_container_width=True)

    st.dataframe(anoms, use_container_width=True)

else:
    st.write("No anomalies")

