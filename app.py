from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.airsafety_ai.pipeline import PROCESSED_DIR, run_pipeline


st.set_page_config(page_title="AirSafety AI Demo", layout="wide")


@st.cache_data
def load_outputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    incident_path = PROCESSED_DIR / "incident_master.csv"
    indicator_path = PROCESSED_DIR / "safety_indicators.csv"
    if not incident_path.exists() or not indicator_path.exists():
        return run_pipeline()
    incidents = pd.read_csv(incident_path, parse_dates=["occurred_at"])
    indicators = pd.read_csv(indicator_path)
    return incidents, indicators


incidents, indicators = load_outputs()
airports = ["All"] + sorted(incidents["airport"].dropna().unique().tolist())
selected_airport = st.sidebar.selectbox("Airport", airports)
selected_df = incidents if selected_airport == "All" else incidents[incidents["airport"] == selected_airport]

st.title("AirSafety AI")
st.caption("Mini-pipeline data / NLP / anomalie pour une demonstration entretien DGAC")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Incidents", len(selected_df))
col2.metric("Atypiques", int(selected_df["is_atypical"].sum()))
col3.metric("Risque moyen", round(selected_df["composite_risk_score"].mean(), 2))
col4.metric("Revues prioritaires", int((selected_df["priority_label"] == "priority_review").sum()))

trend = (
    selected_df.groupby("month", as_index=False)
    .agg(incident_count=("incident_id", "count"), atypical_incidents=("is_atypical", "sum"))
    .sort_values("month")
)
fig_trend = px.line(
    trend,
    x="month",
    y=["incident_count", "atypical_incidents"],
    markers=True,
    title="Suivi des incidents et des cas atypiques",
)
st.plotly_chart(fig_trend, use_container_width=True)

top_airports = (
    indicators.groupby("airport", as_index=False)
    .agg(incident_count=("incident_count", "sum"), atypical_incidents=("atypical_incidents", "sum"))
    .sort_values("incident_count", ascending=False)
)
fig_airports = px.bar(
    top_airports,
    x="airport",
    y="incident_count",
    color="atypical_incidents",
    title="Charge d'incidents par aeroport",
)
st.plotly_chart(fig_airports, use_container_width=True)

st.subheader("Incidents prioritaires")
priority_cols = [
    "incident_id",
    "occurred_at",
    "airport",
    "title",
    "severity_reported",
    "topic_tags",
    "composite_risk_score",
    "priority_label",
]
st.dataframe(
    selected_df[selected_df["priority_label"] == "priority_review"][priority_cols]
    .sort_values(["composite_risk_score", "occurred_at"], ascending=[False, False]),
    use_container_width=True,
)

st.subheader("Exemple de narration entretien")
st.markdown(
    """
    - Ingestion simulee depuis deux sources de type API.
    - ETL Python pour fiabiliser et enrichir les donnees.
    - Brique NLP simple pour extraire des themes dans les comptes rendus.
    - Detection d'incidents atypiques pour aider la priorisation des analyses securite.
    - Sorties tabulaires reutilisables dans Power BI.
    """
)
