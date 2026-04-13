#!/usr/bin/env python3
"""Calcul des indicateurs epidemiologiques IAS."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)


def calculer_zscore(valeur_actuelle: float, historique: list[Optional[float]]) -> Optional[float]:
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        return None
    ecart_type = float(np.std(valeurs_valides, ddof=1))
    if ecart_type == 0:
        return 0.0
    moyenne = float(np.mean(valeurs_valides))
    return float((valeur_actuelle - moyenne) / ecart_type)


def classifier_statut_ias(valeur_ias: float, seuil_min: Optional[float], seuil_max: Optional[float]) -> str:
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(series_hebdomadaire: list[float], duree_infectieuse: int) -> Optional[float]:
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        return None

    croissances = []
    for i in range(1, len(series_valides)):
        prev = series_valides[i - 1]
        curr = series_valides[i]
        croissances.append((curr - prev) / prev)

    if not croissances:
        return None
    return max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7)))


def calculer_indicateurs(donnees_brutes: dict[str, Any], historique_db: dict[str, list[float]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    semaine = donnees_brutes["semaine"]
    syndromes = donnees_brutes.get("syndromes", {})

    for syndrome, payload in syndromes.items():
        valeur_ias = payload.get("valeur_ias")
        if valeur_ias is None:
            continue
        hist_saisons = list(payload.get("historique", {}).values())
        z_score = calculer_zscore(valeur_ias, hist_saisons)
        statut_ias = classifier_statut_ias(valeur_ias, payload.get("seuil_min"), payload.get("seuil_max"))
        statut_z = classifier_statut_zscore(z_score)
        statut = classifier_statut_final(statut_ias, statut_z)
        serie_recentes = (historique_db.get(syndrome, []) + [valeur_ias])[-4:]
        duree = 3 if syndrome == "GEA" else 5
        r0 = calculer_r0_simplifie(serie_recentes, duree_infectieuse=duree)

        out.append(
            {
                "semaine": semaine,
                "syndrome": syndrome,
                "valeur_ias": valeur_ias,
                "z_score": z_score,
                "r0_estime": r0,
                "nb_saisons_reference": len([v for v in hist_saisons if v is not None]),
                "statut": statut,
                "statut_ias": statut_ias,
                "statut_zscore": statut_z,
                "commentaire": f"Indicateurs calcules pour {syndrome}",
                "seuil_min_saison": payload.get("seuil_min"),
                "seuil_max_saison": payload.get("seuil_max"),
                "nb_jours_donnees": payload.get("nb_jours", 0),
            }
        )
    return out


def charger_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def sauvegarder_indicateurs(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return path
