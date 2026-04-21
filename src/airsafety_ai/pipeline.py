from __future__ import annotations

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd


# Chemins locaux et metadonnees de la source officielle DGAC.
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
RAW_ZIP_PATH = RAW_DIR / "dgac_traffic_routes.zip"
DGAC_SOURCE_URL = "https://www.data.gouv.fr/api/1/datasets/r/c89f2fb5-df1f-4a53-adf5-27f719e44137"
DGAC_SOURCE_LABEL = "DGAC - Trafic aerien commercial mensuel francais par paire d'aeroports par sens depuis 1990"
DGAC_SOURCE_UPDATED_AT = "2026-01-07"


def _to_number(series: pd.Series) -> pd.Series:
    """Convertit une serie texte DGAC en numerique exploitable par pandas."""
    # Les fichiers DGAC utilisent parfois des espaces et des virgules decimales.
    # Cette fonction harmonise les formats pour permettre les calculs numeriques.
    cleaned = (
        series.astype(str)
        .str.replace("\u202f", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def load_raw_data(zip_path: Path = RAW_ZIP_PATH) -> pd.DataFrame:
    """Charge tous les CSV annuels contenus dans l'archive officielle DGAC."""
    # On charge l'archive ZIP officielle puis on concatene tous les CSV annuels
    # dans une seule table brute pour simplifier le traitement aval.
    if not zip_path.exists():
        raise FileNotFoundError(
            "Le fichier DGAC officiel est absent. Placez l'archive dans data/raw/dgac_traffic_routes.zip."
        )

    frames: list[pd.DataFrame] = []
    with ZipFile(zip_path) as archive:
        csv_names = sorted(name for name in archive.namelist() if name.endswith(".csv"))
        for name in csv_names:
            with archive.open(name) as handle:
                frame = pd.read_csv(BytesIO(handle.read()), sep=";", dtype=str, encoding="utf-8-sig")
            frame["source_file"] = name
            frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les colonnes brutes DGAC et cree un jeu de donnees analytique lisible."""
    # Cette etape prepare un jeu de donnees propre et lisible pour l'analyse :
    # conversion des dates, nettoyage des mesures et renommage logique des dimensions.
    data = df.copy()
    data["ANMOIS"] = data["ANMOIS"].astype(str)
    data["date"] = pd.to_datetime(data["ANMOIS"] + "01", format="%Y%m%d")
    data["year"] = data["date"].dt.year
    data["month"] = data["date"].dt.month
    data["LSN_PEQ"] = _to_number(data["LSN_PEQ"])
    data["LSN_PAX"] = _to_number(data["LSN_PAX"])
    data["LSN_DRT"] = _to_number(data["LSN_DRT"])
    data["LSN_FRP"] = _to_number(data["LSN_FRP"])
    data["LSN_1"] = data["LSN_1"].fillna("NON_RENSEIGNE")
    data["LSN_2"] = data["LSN_2"].fillna("NON_RENSEIGNE")
    data["LSN_2_CONT"] = data["LSN_2_CONT"].fillna("NON_RENSEIGNE")
    data["LSN_SEG"] = data["LSN_SEG"].fillna("NON_RENSEIGNE")

    # On retire les lignes d'agregation globale pour ne conserver que des liaisons
    # directement exploitables dans le suivi par destination.
    data = data[
        (data["LSN_SEG"] != "NON_RENSEIGNE")
        & (~data["LSN_2"].str.startswith("_"))
        & (~data["LSN_1"].str.startswith("_"))
    ].copy()

    # On cree ensuite des colonnes plus parlantes pour l'analyse et le dashboard.
    data["route_label"] = data["LSN_1"] + " -> " + data["LSN_2"]
    data["segment_label"] = data["LSN_SEG"].map({"INTL": "International", "NAT": "National"}).fillna(data["LSN_SEG"])
    data["origin_zone"] = data["LSN_1"].replace({"MÉTROPOLE": "Metropole", "OUTRE-MER": "Outre-mer"})
    data["destination"] = data["LSN_2"].str.title()
    data["destination_continent"] = data["LSN_2_CONT"].str.title()
    data["passengers"] = data["LSN_PAX"].fillna(0)
    data["commercial_units"] = data["LSN_PEQ"].fillna(0)
    data["direct_flights"] = data["LSN_DRT"].fillna(0)
    return data.sort_values("date").reset_index(drop=True)


def build_monthly_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Construit les indicateurs mensuels de trafic par segment."""
    # Ce bloc produit une vue mensuelle de pilotage par segment
    # pour suivre le trafic, les vols et la diversite des destinations.
    monthly = (
        df.groupby(["date", "year", "month", "segment_label"], as_index=False)
        .agg(
            passengers=("passengers", "sum"),
            commercial_units=("commercial_units", "sum"),
            direct_flights=("direct_flights", "sum"),
            destinations=("destination", "nunique"),
        )
        .sort_values(["date", "segment_label"])
    )
    # Variation par rapport au meme mois de l'annee precedente.
    monthly["passengers_yoy_pct"] = (
        monthly.groupby("segment_label")["passengers"].pct_change(periods=12).mul(100).round(2)
    )
    monthly["direct_flights_yoy_pct"] = (
        monthly.groupby("segment_label")["direct_flights"].pct_change(periods=12).mul(100).round(2)
    )
    return monthly


def build_destination_monitor(df: pd.DataFrame) -> pd.DataFrame:
    """Construit une table de suivi des destinations avec variations et alertes."""
    # On se concentre sur les annees recentes pour une lecture de monitoring plus utile.
    recent = df[df["year"] >= 2019].copy()
    grouped = (
        recent.groupby(
            ["date", "year", "month", "segment_label", "origin_zone", "destination", "destination_continent"],
            as_index=False,
        )
        .agg(passengers=("passengers", "sum"), direct_flights=("direct_flights", "sum"))
        .sort_values(["destination", "date"])
    )
    # Reference N-1 pour mesurer les hausses ou baisses significatives.
    grouped["passengers_last_year"] = grouped.groupby(["origin_zone", "destination", "segment_label"])[
        "passengers"
    ].shift(12)
    grouped["direct_flights_last_year"] = grouped.groupby(["origin_zone", "destination", "segment_label"])[
        "direct_flights"
    ].shift(12)
    grouped["passengers_yoy_pct"] = (
        (grouped["passengers"] - grouped["passengers_last_year"]) / grouped["passengers_last_year"]
    ) * 100
    grouped["direct_flights_yoy_pct"] = (
        (grouped["direct_flights"] - grouped["direct_flights_last_year"]) / grouped["direct_flights_last_year"]
    ) * 100

    # Moyenne glissante sur 3 mois pour comparer le mois courant au rythme recent.
    grouped["rolling_3m_passengers"] = (
        grouped.groupby(["origin_zone", "destination", "segment_label"])["passengers"]
        .transform(lambda s: s.rolling(3, min_periods=3).mean())
    )
    grouped["rolling_gap_pct"] = (
        (grouped["passengers"] - grouped["rolling_3m_passengers"]) / grouped["rolling_3m_passengers"]
    ) * 100

    # Regle simple de mise sous surveillance :
    # volume suffisant et variation importante soit vs N-1, soit vs tendance recente.
    grouped["alert_level"] = np.where(
        (grouped["passengers"] >= 20000)
        & (
            (grouped["passengers_yoy_pct"].abs().fillna(0) >= 25)
            | (grouped["rolling_gap_pct"].abs().fillna(0) >= 20)
        ),
        "watch",
        "normal",
    )
    # Libelle explicatif pour rendre l'alerte lisible par un utilisateur metier.
    grouped["alert_reason"] = np.select(
        [
            grouped["passengers_yoy_pct"].fillna(0) <= -25,
            grouped["passengers_yoy_pct"].fillna(0) >= 25,
            grouped["rolling_gap_pct"].fillna(0) <= -20,
            grouped["rolling_gap_pct"].fillna(0) >= 20,
        ],
        [
            "Baisse marquee vs N-1",
            "Hausse marquee vs N-1",
            "Sous le rythme recent",
            "Au-dessus du rythme recent",
        ],
        default="RAS",
    )
    return grouped


def build_latest_alerts(destination_monitor: pd.DataFrame) -> pd.DataFrame:
    """Extrait les alertes a surveiller sur le dernier mois disponible."""
    # Cette vue isole uniquement le dernier mois disponible et trie les liaisons
    # les plus notables pour une lecture executive rapide.
    latest_date = destination_monitor["date"].max()
    alerts = destination_monitor[
        (destination_monitor["date"] == latest_date) & (destination_monitor["alert_level"] == "watch")
    ].copy()
    alerts["abs_yoy"] = alerts["passengers_yoy_pct"].abs().fillna(0)
    alerts = alerts.sort_values(["abs_yoy", "passengers"], ascending=[False, False])
    return alerts.drop(columns=["abs_yoy"])


def build_executive_summary(df: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    """Genere un resume de contexte sur la source et la couverture temporelle."""
    # Resume leger ajoute a la sortie pour rappeler la source, la fraicheur
    # et la periode couverte par le projet.
    latest_date = monthly["date"].max()
    latest_month = monthly[monthly["date"] == latest_date].copy()
    latest_month["source_label"] = DGAC_SOURCE_LABEL
    latest_month["source_updated_at"] = DGAC_SOURCE_UPDATED_AT
    latest_month["source_url"] = DGAC_SOURCE_URL
    latest_month["latest_data_month"] = latest_date.strftime("%Y-%m")
    latest_month["covered_years"] = f"{int(df['year'].min())}-{int(df['year'].max())}"
    return latest_month


def export_outputs(
    traffic_master: pd.DataFrame,
    monthly: pd.DataFrame,
    destination_monitor: pd.DataFrame,
    latest_alerts: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    """Ecrit les tables finales dans data/processed pour le dashboard et le partage."""
    # Export des tables finales afin de separer clairement le calcul
    # de la couche de restitution Streamlit.
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    traffic_master.to_csv(PROCESSED_DIR / "traffic_master.csv", index=False)
    monthly.to_csv(PROCESSED_DIR / "monthly_indicators.csv", index=False)
    destination_monitor.to_csv(PROCESSED_DIR / "destination_monitor.csv", index=False)
    latest_alerts.to_csv(PROCESSED_DIR / "latest_alerts.csv", index=False)
    summary.to_csv(PROCESSED_DIR / "executive_summary.csv", index=False)


def run_pipeline() -> dict[str, pd.DataFrame]:
    """Execute l'ensemble du pipeline et retourne les tables principales."""
    # Orchestration complete du pipeline : lecture, transformation,
    # calcul des indicateurs, detection des alertes puis export.
    raw = load_raw_data()
    traffic_master = transform(raw)
    monthly = build_monthly_indicators(traffic_master)
    destination_monitor = build_destination_monitor(traffic_master)
    latest_alerts = build_latest_alerts(destination_monitor)
    summary = build_executive_summary(traffic_master, monthly)
    export_outputs(traffic_master, monthly, destination_monitor, latest_alerts, summary)
    return {
        "traffic_master": traffic_master,
        "monthly_indicators": monthly,
        "destination_monitor": destination_monitor,
        "latest_alerts": latest_alerts,
        "executive_summary": summary,
    }


if __name__ == "__main__":
    # Execution directe en ligne de commande pour regenerer les sorties.
    outputs = run_pipeline()
    master = outputs["traffic_master"]
    alerts = outputs["latest_alerts"]
    print(
        "Pipeline completed: "
        f"{len(master)} monthly route records, "
        f"{master['year'].min()}-{master['year'].max()} coverage, "
        f"{len(alerts)} latest alerts."
    )
