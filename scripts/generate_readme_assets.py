from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ASSETS_DIR = ROOT / "assets"
PROCESSED_DIR = ROOT / "data" / "processed"


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    monthly = pd.read_csv(PROCESSED_DIR / "monthly_indicators.csv", parse_dates=["date"])
    alerts = pd.read_csv(PROCESSED_DIR / "latest_alerts.csv", parse_dates=["date"])
    summary = pd.read_csv(PROCESSED_DIR / "executive_summary.csv")
    return monthly, alerts, summary


def build_overview_image(monthly: pd.DataFrame, alerts: pd.DataFrame, summary: pd.DataFrame) -> None:
    latest_month = summary["latest_data_month"].iloc[0]
    total_passengers = int(summary["passengers"].sum())
    total_flights = int(summary["direct_flights"].sum())
    destinations = int(monthly[monthly["date"] == monthly["date"].max()]["destinations"].sum())
    watch_count = len(alerts)

    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.patch.set_facecolor("#f7f3ea")
    cards = [
        ("Dernier mois", latest_month, "#183a37"),
        ("Passagers", f"{total_passengers:,}".replace(",", " "), "#2f6690"),
        ("Vols directs", f"{total_flights:,}".replace(",", " "), "#d17a22"),
        ("Liaisons en veille", str(watch_count), "#8f2d56"),
    ]

    for ax, (title, value, color) in zip(axes.flatten(), cards):
        ax.set_facecolor(color)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.text(0.06, 0.72, title, fontsize=18, color="white", fontweight="bold", transform=ax.transAxes)
        ax.text(0.06, 0.33, value, fontsize=28, color="white", fontweight="bold", transform=ax.transAxes)

    fig.suptitle(
        f"DGAC Traffic Intelligence\n{destinations} destinations suivies sur le dernier mois disponible",
        fontsize=22,
        fontweight="bold",
        y=0.98,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(ASSETS_DIR / "dashboard_overview.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def build_trend_image(monthly: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("white")

    for segment, color in [("International", "#24577a"), ("National", "#c97b2a")]:
        subset = monthly[monthly["segment_label"] == segment].sort_values("date")
        ax.plot(subset["date"], subset["passengers"], label=segment, linewidth=2.5, color=color)

    ax.set_title("Evolution mensuelle des passagers", fontsize=18, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Passagers")
    ax.grid(alpha=0.2)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(ASSETS_DIR / "dashboard_trend.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def build_alerts_image(alerts: pd.DataFrame) -> None:
    top_alerts = alerts.sort_values(["passengers", "passengers_yoy_pct"], ascending=[False, False]).head(10).copy()
    top_alerts["route"] = top_alerts["origin_zone"] + " -> " + top_alerts["destination"]
    top_alerts = top_alerts.sort_values("passengers")

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("white")
    colors = ["#1f7a8c" if value >= 0 else "#b23a48" for value in top_alerts["passengers_yoy_pct"].fillna(0)]
    ax.barh(top_alerts["route"], top_alerts["passengers"], color=colors)
    ax.set_title("Top liaisons a surveiller sur le dernier mois", fontsize=18, fontweight="bold")
    ax.set_xlabel("Passagers")
    ax.set_ylabel("")
    ax.grid(axis="x", alpha=0.2)

    for idx, (_, row) in enumerate(top_alerts.iterrows()):
        yoy = row["passengers_yoy_pct"]
        label = f"{yoy:+.1f}% | {row['alert_reason']}"
        ax.text(row["passengers"] * 1.01, idx, label, va="center", fontsize=9)

    fig.tight_layout()
    fig.savefig(ASSETS_DIR / "dashboard_alerts.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ASSETS_DIR.mkdir(exist_ok=True)
    monthly, alerts, summary = _load_data()
    build_overview_image(monthly, alerts, summary)
    build_trend_image(monthly)
    build_alerts_image(alerts)
    print("README assets generated in assets/")


if __name__ == "__main__":
    main()
