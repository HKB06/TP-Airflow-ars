from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/scripts")

from calcul_indicateurs import calculer_indicateurs, charger_json, sauvegarder_indicateurs  # noqa: E402
from collecte_sursaud import collecter_donnees_hebdo  # noqa: E402

logger = logging.getLogger(__name__)


def _semaine_from_context(context: dict[str, Any]) -> str:
    execution_date = context["execution_date"]
    return f"{execution_date.year}-S{execution_date.isocalendar()[1]:02d}"


def collecter_donnees_sursaud(**context: Any) -> str:
    try:
        semaine = _semaine_from_context(context)
        archive_path = Variable.get("archive_base_path", default_var="/data/ars")
        output_dir = f"{archive_path}/raw"
        return collecter_donnees_hebdo(semaine=semaine, output_dir=output_dir)
    except Exception:
        logger.exception("Echec de la collecte SurSaUD/IAS")
        raise


def archiver_local(**context: Any) -> str:
    try:
        semaine = _semaine_from_context(context)
        annee, num_sem = semaine.split("-")
        chemin_source = context["task_instance"].xcom_pull(task_ids="collecte.collecter_donnees_sursaud")
        archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
        os.makedirs(archive_dir, exist_ok=True)
        chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
        shutil.copy2(chemin_source, chemin_dest)
        return chemin_dest
    except Exception:
        logger.exception("Echec de l'archivage local")
        raise


def verifier_archive(**context: Any) -> bool:
    try:
        semaine = _semaine_from_context(context)
        annee, num_sem = semaine.split("-")
        chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
        if not os.path.exists(chemin):
            raise FileNotFoundError(f"Archive manquante: {chemin}")
        if os.path.getsize(chemin) == 0:
            raise ValueError(f"Archive vide: {chemin}")
        return True
    except Exception:
        logger.exception("Echec de verification de l'archive")
        raise


def calculer_indicateurs_epidemiques(**context: Any) -> str:
    try:
        semaine = _semaine_from_context(context)
        annee, num_sem = semaine.split("-")
        path_raw = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
        donnees_brutes = charger_json(path_raw)

        hook = PostgresHook(postgres_conn_id="postgres_ars")
        historique_db: dict[str, list[float]] = {}
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT syndrome, valeur_ias
                    FROM donnees_hebdomadaires
                    WHERE semaine < %s
                    ORDER BY semaine DESC
                    LIMIT 20
                    """,
                    (semaine,),
                )
                for syndrome, valeur in cur.fetchall():
                    historique_db.setdefault(syndrome, []).append(float(valeur))

        indicateurs = calculer_indicateurs(donnees_brutes, historique_db)
        payload = {"semaine": semaine, "indicateurs": indicateurs}
        path_out = f"/data/ars/indicateurs/indicateurs_{semaine}.json"
        return sauvegarder_indicateurs(path_out, payload)
    except Exception:
        logger.exception("Echec du calcul d'indicateurs")
        raise


def inserer_donnees_postgres(**context: Any) -> None:
    try:
        semaine = _semaine_from_context(context)
        annee, num_sem = semaine.split("-")
        raw_path = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
        ind_path = f"/data/ars/indicateurs/indicateurs_{semaine}.json"
        donnees_brutes = charger_json(raw_path)
        indicateurs = charger_json(ind_path).get("indicateurs", [])

        hook = PostgresHook(postgres_conn_id="postgres_ars")

        sql_donnees = """
        INSERT INTO donnees_hebdomadaires
        (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min_saison)s, %(seuil_max_saison)s, %(nb_jours_donnees)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
          valeur_ias = EXCLUDED.valeur_ias,
          seuil_min_saison = EXCLUDED.seuil_min_saison,
          seuil_max_saison = EXCLUDED.seuil_max_saison,
          nb_jours_donnees = EXCLUDED.nb_jours_donnees,
          updated_at = CURRENT_TIMESTAMP
        """

        sql_indicateurs = """
        INSERT INTO indicateurs_epidemiques
        (semaine, syndrome, valeur_ias, z_score, r0_estime, nb_saisons_reference, statut, statut_ias, statut_zscore, commentaire)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s, %(r0_estime)s, %(nb_saisons_reference)s, %(statut)s, %(statut_ias)s, %(statut_zscore)s, %(commentaire)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
          valeur_ias = EXCLUDED.valeur_ias,
          z_score = EXCLUDED.z_score,
          r0_estime = EXCLUDED.r0_estime,
          nb_saisons_reference = EXCLUDED.nb_saisons_reference,
          statut = EXCLUDED.statut,
          statut_ias = EXCLUDED.statut_ias,
          statut_zscore = EXCLUDED.statut_zscore,
          commentaire = EXCLUDED.commentaire,
          updated_at = CURRENT_TIMESTAMP
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for syndrome, payload in donnees_brutes.get("syndromes", {}).items():
                    if payload.get("valeur_ias") is None:
                        continue
                    cur.execute(
                        sql_donnees,
                        {
                            "semaine": semaine,
                            "syndrome": syndrome,
                            "valeur_ias": payload.get("valeur_ias"),
                            "seuil_min_saison": payload.get("seuil_min"),
                            "seuil_max_saison": payload.get("seuil_max"),
                            "nb_jours_donnees": payload.get("nb_jours", 0),
                        },
                    )
                for indicateur in indicateurs:
                    cur.execute(sql_indicateurs, indicateur)
                conn.commit()
    except Exception:
        logger.exception("Echec insertion PostgreSQL")
        raise


def evaluer_situation_epidemique(**context: Any) -> str:
    try:
        semaine = _semaine_from_context(context)
        hook = PostgresHook(postgres_conn_id="postgres_ars")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT statut, COUNT(*) AS nb
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    GROUP BY statut
                    """,
                    (semaine,),
                )
                resultats = {row[0]: row[1] for row in cur.fetchall()}

        nb_urgence = int(resultats.get("URGENCE", 0))
        nb_alerte = int(resultats.get("ALERTE", 0))
        context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
        context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)

        if nb_urgence > 0:
            return "declencher_alerte_ars"
        if nb_alerte > 0:
            return "envoyer_bulletin_surveillance"
        return "confirmer_situation_normale"
    except Exception:
        logger.exception("Echec de l'evaluation de situation")
        raise


def declencher_alerte_ars(**context: Any) -> None:
    nb_urgence = context["task_instance"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_urgence")
    logger.critical("ALERTE ARS: %s syndrome(s) en URGENCE", nb_urgence)


def envoyer_bulletin_surveillance(**context: Any) -> None:
    nb_alerte = context["task_instance"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_alerte")
    logger.warning("Bulletin surveillance: %s syndrome(s) en ALERTE", nb_alerte)


def confirmer_situation_normale() -> None:
    logger.info("Situation epidemiologique normale")


def generer_rapport_hebdomadaire(**context: Any) -> None:
    try:
        semaine = _semaine_from_context(context)
        hook = PostgresHook(postgres_conn_id="postgres_ars")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT syndrome, valeur_ias, z_score, r0_estime, statut
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    ORDER BY statut DESC, syndrome
                    """,
                    (semaine,),
                )
                rows = cur.fetchall()

        statuts = [row[4] for row in rows]
        if "URGENCE" in statuts:
            situation_globale = "URGENCE"
        elif "ALERTE" in statuts:
            situation_globale = "ALERTE"
        else:
            situation_globale = "NORMAL"

        rapport = {
            "semaine": semaine,
            "region": "Occitanie",
            "code_region": "76",
            "date_generation": datetime.utcnow().isoformat(),
            "situation_globale": situation_globale,
            "nb_departements_surveilles": 13,
            "departements_en_urgence": [],
            "departements_en_alerte": [],
            "indicateurs": [
                {
                    "syndrome": row[0],
                    "valeur_ias": row[1],
                    "z_score": row[2],
                    "r0_estime": row[3],
                    "statut": row[4],
                }
                for row in rows
            ],
            "genere_par": "ars_epidemio_dag v1.0",
            "pipeline_version": "2.8",
        }

        annee, num_sem = semaine.split("-")
        local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as file:
            json.dump(rapport, file, ensure_ascii=False, indent=2)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rapports_ars
                    (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence, rapport_json, chemin_local)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (semaine) DO UPDATE SET
                      situation_globale = EXCLUDED.situation_globale,
                      nb_depts_alerte = EXCLUDED.nb_depts_alerte,
                      nb_depts_urgence = EXCLUDED.nb_depts_urgence,
                      rapport_json = EXCLUDED.rapport_json,
                      chemin_local = EXCLUDED.chemin_local,
                      updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        semaine,
                        situation_globale,
                        1 if situation_globale in ("ALERTE", "URGENCE") else 0,
                        1 if situation_globale == "URGENCE" else 0,
                        json.dumps(rapport, ensure_ascii=False),
                        local_path,
                    ),
                )
                conn.commit()
    except Exception:
        logger.exception("Echec generation du rapport")
        raise


default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance epidemiologique ARS Occitanie",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:
    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    with TaskGroup(group_id="collecte") as collecte:
        collecter = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_sursaud,
        )

    with TaskGroup(group_id="persistance_brute") as persistance_brute:
        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
        )
        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
        )
        archiver >> verifier

    with TaskGroup(group_id="traitement") as traitement:
        calculer = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
        )

    with TaskGroup(group_id="persistance_operationnelle") as persistance_operationnelle:
        inserer = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
        )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
    )
    alerte = PythonOperator(task_id="declencher_alerte_ars", python_callable=declencher_alerte_ars)
    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
    )
    normal = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
    )

    rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    init_base_donnees >> collecte >> persistance_brute >> traitement >> persistance_operationnelle >> evaluer
    evaluer >> [alerte, bulletin, normal] >> rapport
