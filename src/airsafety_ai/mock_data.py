from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


RAW_DIR = Path("data/raw")


@dataclass(frozen=True)
class IncidentTemplate:
    title: str
    description: str
    category: str
    phase: str
    severity: str


TEMPLATES = [
    IncidentTemplate(
        title="Instabilite en approche",
        description="Approche non stabilisee avec correction tardive et charge de travail elevee.",
        category="flight_path",
        phase="approach",
        severity="medium",
    ),
    IncidentTemplate(
        title="Perte separation mineure",
        description="Ecart de separation detecte par le controle puis resolu sans manoeuvre brusque.",
        category="separation",
        phase="cruise",
        severity="high",
    ),
    IncidentTemplate(
        title="Alerte oiseau au decollage",
        description="Presence d'oiseaux signalee en piste avec impact potentiel au decollage.",
        category="wildlife",
        phase="takeoff",
        severity="medium",
    ),
    IncidentTemplate(
        title="Message equipage ambigu",
        description="Phraseologie ambiguë observee entre equipage et controle aerien.",
        category="communication",
        phase="climb",
        severity="low",
    ),
    IncidentTemplate(
        title="Parametre moteur anormal",
        description="Variation anormale d'un parametre moteur suivie d'un retour a la normale.",
        category="technical",
        phase="cruise",
        severity="high",
    ),
    IncidentTemplate(
        title="Alerte carburant",
        description="Marge carburant reduite a l'arrivee apres reroutage meteorologique.",
        category="fuel",
        phase="descent",
        severity="high",
    ),
]


AIRPORTS = ["LFPG", "LFPO", "LFBO", "LFML", "LFMN", "LFLL"]
WEATHERS = ["VMC", "IMC", "WINDY", "STORM"]
AIRCRAFT = ["A320", "A321", "B737", "E190", "ATR72"]
OPERATORS = ["AFR", "EZY", "TVF", "RYR", "HOP"]


def _random_description(template: IncidentTemplate, rng: random.Random) -> str:
    suffixes = [
        "L'equipage a applique la procedure standard.",
        "Le controle a emis une consigne corrective.",
        "Une verification supplementaire a ete demandee.",
        "L'evenement a ete classe pour revue securite.",
        "Le commandant a signale un facteur humain contributif.",
    ]
    return f"{template.description} {rng.choice(suffixes)}"


def generate_mock_api_payloads(seed: int = 42, n_records: int = 240) -> None:
    rng = random.Random(seed)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start = datetime(2025, 1, 1, 6, 0, 0)
    reports = []
    flights = []

    for index in range(n_records):
        template = rng.choice(TEMPLATES)
        flight_id = f"FLIGHT-{1000 + index}"
        incident_id = f"INC-{2025000 + index}"
        occurred_at = start + timedelta(hours=index * 8 + rng.randint(0, 4))
        airport = rng.choice(AIRPORTS)
        weather = rng.choices(WEATHERS, weights=[0.55, 0.2, 0.18, 0.07], k=1)[0]
        altitude = rng.randint(0, 380)
        occupancy = rng.randint(40, 220)

        severity = template.severity
        if weather == "STORM" and severity != "high":
            severity = "high"

        reports.append(
            {
                "incident_id": incident_id,
                "flight_id": flight_id,
                "title": template.title,
                "description": _random_description(template, rng),
                "category_reported": template.category,
                "phase_of_flight": template.phase,
                "severity_reported": severity,
                "airport": airport,
                "status": rng.choice(["open", "closed", "under_review"]),
                "occurred_at": occurred_at.isoformat(),
            }
        )

        flights.append(
            {
                "flight_id": flight_id,
                "operator": rng.choice(OPERATORS),
                "aircraft_type": rng.choice(AIRCRAFT),
                "weather_context": weather,
                "departure_airport": airport,
                "arrival_airport": rng.choice([a for a in AIRPORTS if a != airport]),
                "altitude_hundreds_ft": altitude,
                "occupancy_estimate": occupancy,
            }
        )

    with (RAW_DIR / "incident_reports.json").open("w", encoding="utf-8") as handle:
        json.dump(reports, handle, indent=2, ensure_ascii=True)

    with (RAW_DIR / "flight_context.json").open("w", encoding="utf-8") as handle:
        json.dump(flights, handle, indent=2, ensure_ascii=True)


if __name__ == "__main__":
    generate_mock_api_payloads()
