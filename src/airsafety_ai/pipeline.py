from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from .mock_data import RAW_DIR, generate_mock_api_payloads


PROCESSED_DIR = Path("data/processed")

SEVERITY_MAP = {"low": 1, "medium": 2, "high": 3}

KEYWORD_MAP = {
    "bird": "wildlife",
    "oiseau": "wildlife",
    "carburant": "fuel",
    "moteur": "technical",
    "separation": "separation",
    "phraseologie": "communication",
    "approche": "flight_path",
    "facteur humain": "human_factors",
}


def load_raw_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not (RAW_DIR / "incident_reports.json").exists():
        generate_mock_api_payloads()

    with (RAW_DIR / "incident_reports.json").open(encoding="utf-8") as handle:
        incidents = pd.DataFrame(json.load(handle))

    with (RAW_DIR / "flight_context.json").open(encoding="utf-8") as handle:
        flights = pd.DataFrame(json.load(handle))

    return incidents, flights


def extract_topics(text: str) -> str:
    lowered = text.lower()
    matches = [topic for keyword, topic in KEYWORD_MAP.items() if keyword in lowered]
    return ", ".join(sorted(set(matches))) if matches else "other"


def compute_text_risk(text: str) -> int:
    lowered = text.lower()
    risk_terms = ["anormal", "alerte", "instable", "perte", "ambigu", "reduit"]
    return sum(term in lowered for term in risk_terms)


def transform(incidents: pd.DataFrame, flights: pd.DataFrame) -> pd.DataFrame:
    df = incidents.merge(flights, on="flight_id", how="left", validate="one_to_one")
    df["occurred_at"] = pd.to_datetime(df["occurred_at"], utc=False)
    df["severity_score"] = df["severity_reported"].map(SEVERITY_MAP).fillna(1)
    df["topic_tags"] = df["description"].apply(extract_topics)
    df["text_risk_score"] = df["description"].apply(compute_text_risk)
    df["is_under_review"] = (df["status"] == "under_review").astype(int)
    df["weather_risk"] = df["weather_context"].map({"VMC": 0, "IMC": 1, "WINDY": 1, "STORM": 2}).fillna(0)
    df["composite_risk_score"] = (
        df["severity_score"] * 2
        + df["text_risk_score"]
        + df["weather_risk"]
        + df["is_under_review"]
    )
    df["month"] = df["occurred_at"].dt.to_period("M").astype(str)
    return df.sort_values("occurred_at").reset_index(drop=True)


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    features = df[
        ["severity_score", "text_risk_score", "weather_risk", "altitude_hundreds_ft", "occupancy_estimate"]
    ].fillna(0)
    model = IsolationForest(contamination=0.08, random_state=42)
    df = df.copy()
    df["anomaly_flag"] = model.fit_predict(features)
    df["is_atypical"] = (df["anomaly_flag"] == -1).astype(int)
    df["priority_label"] = np.where(
        (df["is_atypical"] == 1) | (df["composite_risk_score"] >= 8),
        "priority_review",
        "standard_review",
    )
    return df


def build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    indicators = (
        df.groupby(["month", "airport"], as_index=False)
        .agg(
            incident_count=("incident_id", "count"),
            average_risk=("composite_risk_score", "mean"),
            atypical_incidents=("is_atypical", "sum"),
            high_severity=("severity_score", lambda s: int((s >= 3).sum())),
        )
        .sort_values(["month", "incident_count"], ascending=[True, False])
    )
    indicators["average_risk"] = indicators["average_risk"].round(2)
    return indicators


def export_outputs(df: pd.DataFrame, indicators: pd.DataFrame) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_DIR / "incident_master.csv", index=False)
    indicators.to_csv(PROCESSED_DIR / "safety_indicators.csv", index=False)
    priority = df[df["priority_label"] == "priority_review"].sort_values(
        ["composite_risk_score", "occurred_at"], ascending=[False, False]
    )
    priority.to_csv(PROCESSED_DIR / "priority_alerts.csv", index=False)


def run_pipeline() -> tuple[pd.DataFrame, pd.DataFrame]:
    incidents, flights = load_raw_data()
    transformed = transform(incidents, flights)
    scored = detect_anomalies(transformed)
    indicators = build_indicators(scored)
    export_outputs(scored, indicators)
    return scored, indicators


if __name__ == "__main__":
    incidents_df, indicators_df = run_pipeline()
    print(f"Pipeline completed: {len(incidents_df)} incidents, {len(indicators_df)} indicator rows exported.")
