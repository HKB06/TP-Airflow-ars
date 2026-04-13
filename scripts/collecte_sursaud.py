#!/usr/bin/env python3
"""Collecte IAS OpenData et aggregation hebdomadaire Occitanie."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date, datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

DATASETS_IAS: dict[str, str] = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA": "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

COLS_OCCITANIE: list[str] = ["Loc_Reg91", "Loc_Reg73"]
SAISONS_COLS: list[str] = [
    "Sais_2023_2024",
    "Sais_2022_2023",
    "Sais_2021_2022",
    "Sais_2020_2021",
    "Sais_2019_2020",
]


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    if reference_date is None:
        reference_date = date.today()
    year, week, _ = reference_date.isocalendar()
    return f"{year}-S{week:02d}"


def _to_float(value: Optional[str]) -> Optional[float]:
    if value in (None, "", "NA"):
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def telecharger_csv_ias(url: str) -> list[dict[str, Any]]:
    logger.info("Telechargement CSV IAS: %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    rows = [dict(row) for row in reader]
    logger.info("Lignes recuperees: %s", len(rows))
    return rows


def filtrer_semaine(rows: list[dict[str, Any]], semaine: str) -> list[dict[str, Any]]:
    annee_cible = int(semaine.split("-")[0])
    sem_cible = int(semaine.split("-S")[1])
    filtered: list[dict[str, Any]] = []

    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue
        try:
            d = datetime.strptime(periode, "%d-%m-%Y").date()
        except ValueError:
            continue
        iso_year, iso_week, _ = d.isocalendar()
        if iso_year == annee_cible and iso_week == sem_cible:
            filtered.append(row)

    logger.info("Jours filtres semaine %s: %s", semaine, len(filtered))
    return filtered


def _safe_mean(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 3) if values else None


def agreger_semaine(rows: list[dict[str, Any]], syndrome: str, semaine: str) -> dict[str, Any]:
    valeurs_ias: list[float] = []
    seuil_min: list[float] = []
    seuil_max: list[float] = []
    historique: dict[str, list[float]] = {col: [] for col in SAISONS_COLS}

    for row in rows:
        vals_occ = [_to_float(row.get(col)) for col in COLS_OCCITANIE]
        vals_occ_clean = [v for v in vals_occ if v is not None]
        if vals_occ_clean:
            valeurs_ias.append(sum(vals_occ_clean) / len(vals_occ_clean))

        min_val = _to_float(row.get("MIN_Saison"))
        max_val = _to_float(row.get("MAX_Saison"))
        if min_val is not None:
            seuil_min.append(min_val)
        if max_val is not None:
            seuil_max.append(max_val)

        for col in SAISONS_COLS:
            value = _to_float(row.get(col))
            if value is not None:
                historique[col].append(value)

    return {
        "semaine": semaine,
        "syndrome": syndrome,
        "valeur_ias": _safe_mean(valeurs_ias),
        "seuil_min": _safe_mean(seuil_min),
        "seuil_max": _safe_mean(seuil_max),
        "nb_jours": len(valeurs_ias),
        "historique": {col: _safe_mean(vals) for col, vals in historique.items()},
    }


def sauvegarder_donnees(donnees: dict[str, Any], semaine: str, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"sursaud_{semaine}.json")
    payload = {
        "semaine": semaine,
        "collecte_le": datetime.utcnow().isoformat(),
        "source": "IAS_OpenHealth_data.gouv.fr",
        "syndromes": donnees,
    }
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    logger.info("Fichier brut ecrit: %s", output_path)
    return output_path


def collecter_donnees_hebdo(semaine: Optional[str], output_dir: str) -> str:
    semaine_cible = semaine or get_semaine_iso()
    resultats: dict[str, Any] = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine_cible)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine_cible)
    return sauvegarder_donnees(resultats, semaine_cible, output_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    semaine_env = os.environ.get("SEMAINE_CIBLE")
    output = os.environ.get("OUTPUT_DIR", "/data/ars/raw")
    fichier = collecter_donnees_hebdo(semaine_env, output)
    print(f"COLLECTE_OK:{fichier}")
