from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.airsafety_ai.pipeline import DGAC_SOURCE_LABEL, DGAC_SOURCE_UPDATED_AT, PROCESSED_DIR, run_pipeline


# Configuration globale de la page Streamlit.
st.set_page_config(page_title="DGAC Traffic Intelligence", layout="wide")


@st.cache_data
def load_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Charge les sorties du pipeline ou les regenere si elles sont absentes."""
    # L'application lit d'abord les exports deja calcules.
    # Si les fichiers n'existent pas, elle relance automatiquement le pipeline.
    monthly_path = PROCESSED_DIR / "monthly_indicators.csv"
    destination_path = PROCESSED_DIR / "destination_monitor.csv"
    alerts_path = PROCESSED_DIR / "latest_alerts.csv"
    summary_path = PROCESSED_DIR / "executive_summary.csv"
    if not all(path.exists() for path in [monthly_path, destination_path, alerts_path, summary_path]):
        outputs = run_pipeline()
        return (
            outputs["monthly_indicators"],
            outputs["destination_monitor"],
            outputs["latest_alerts"],
            outputs["executive_summary"],
        )
    return (
        pd.read_csv(monthly_path, parse_dates=["date"]),
        pd.read_csv(destination_path, parse_dates=["date"]),
        pd.read_csv(alerts_path, parse_dates=["date"]),
        pd.read_csv(summary_path),
    )


# Chargement principal des jeux de donnees utilises par le dashboard.
monthly, destination_monitor, latest_alerts, executive_summary = load_outputs()

# Filtre lateral pour focaliser l'analyse sur un segment de trafic.
segment_options = ["Tous"] + sorted(monthly["segment_label"].dropna().unique().tolist())
selected_segment = st.sidebar.selectbox("Segment", segment_options)
filtered_monthly = monthly if selected_segment == "Tous" else monthly[monthly["segment_label"] == selected_segment]
filtered_destinations = (
    destination_monitor if selected_segment == "Tous" else destination_monitor[destination_monitor["segment_label"] == selected_segment]
)

# Preparation des vues affichees dans les indicateurs et graphiques.
latest_month = filtered_monthly["date"].max()
latest_snapshot = filtered_monthly[filtered_monthly["date"] == latest_month]
top_destinations = (
    filtered_destinations[filtered_destinations["date"] == latest_month]
    .sort_values("passengers", ascending=False)
    .head(15)
)
alerts_snapshot = (
    latest_alerts if selected_segment == "Tous" else latest_alerts[latest_alerts["segment_label"] == selected_segment]
)

st.title("DGAC Traffic Intelligence")
st.caption(
    f"Source officielle: {DGAC_SOURCE_LABEL} | mise a jour source: {DGAC_SOURCE_UPDATED_AT} | dernier mois disponible: {latest_month:%Y-%m}"
)

# Tuiles de synthese pour une lecture executive immediate.
col1, col2, col3, col4 = st.columns(4)
col1.metric("Passagers dernier mois", f"{int(latest_snapshot['passengers'].sum()):,}".replace(",", " "))
col2.metric("Vols directs", f"{int(latest_snapshot['direct_flights'].sum()):,}".replace(",", " "))
col3.metric("Destinations suivies", int(filtered_destinations[filtered_destinations["date"] == latest_month]["destination"].nunique()))
col4.metric("Liaisons en veille", int(len(alerts_snapshot)))

# Evolution mensuelle du trafic pour visualiser la tendance generale.
trend = px.line(
    filtered_monthly.sort_values("date"),
    x="date",
    y="passengers",
    color="segment_label",
    markers=True,
    title="Evolution mensuelle des passagers",
)
st.plotly_chart(trend, use_container_width=True)

# Classement du dernier mois pour montrer les destinations dominantes
# et leur evolution par rapport a l'annee precedente.
growth_chart = px.bar(
    top_destinations,
    x="destination",
    y="passengers",
    color="passengers_yoy_pct",
    color_continuous_scale="RdYlGn",
    title="Top destinations du dernier mois et variation vs N-1",
)
st.plotly_chart(growth_chart, use_container_width=True)

st.subheader("Liaisons a surveiller")
# Tableau de suivi prioritaire avec les liaisons signalees par les regles d'alerte.
alert_cols = [
    "date",
    "origin_zone",
    "destination",
    "destination_continent",
    "segment_label",
    "passengers",
    "direct_flights",
    "passengers_yoy_pct",
    "rolling_gap_pct",
    "alert_reason",
]
st.dataframe(
    alerts_snapshot[alert_cols].sort_values(["passengers", "passengers_yoy_pct"], ascending=[False, False]),
    use_container_width=True,
)

st.subheader("Lecture entretien")
# Bloc de narration pour aider un lecteur externe a comprendre rapidement
# ce que couvre le projet sans devoir inspecter le code.
st.markdown(
    """
    - Ingestion d'une source ouverte officielle publiee par la DGAC sur data.gouv.fr.
    - ETL Python pour normaliser plusieurs fichiers CSV historiques inclus dans une archive ZIP.
    - Construction d'indicateurs mensuels de trafic et de vols directs.
    - Mise sous surveillance de liaisons presentant des variations atypiques vs N-1 ou vs tendance recente.
    - Restitution exploitable pour un usage de pilotage ou une reprise dans Power BI.
    """
)
