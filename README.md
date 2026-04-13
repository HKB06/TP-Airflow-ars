# TP Airflow - Data Platform Sante Publique ARS Occitanie

## Auteur
- Nom : Khaled Brotons
- Prenom : Hugo
- Date : 13/04/2026

## Prerequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0

## Instructions de deploiement

### 1) Preparation
```powershell
Copy-Item .env.example .env
```

Si besoin sous Windows:
```powershell
Set-Content -Path .env -Value "AIRFLOW_UID=50000"
```

### 2) Demarrage de la stack
```powershell
docker compose up -d --build
docker compose ps
```

### 3) Acces aux interfaces
- Airflow UI: http://localhost:8080 (admin / admin)
- Flower: http://localhost:5555
- PostgreSQL ARS: localhost:5433 (postgres / postgres / ars_epidemio)

### 4) Configuration Airflow
#### Connection
- Conn Id: `postgres_ars`
- Conn Type: `Postgres`
- Host: `postgres-ars`
- Database: `ars_epidemio`
- Login: `postgres`
- Password: `postgres`
- Port: `5432`

#### Variables
- `archive_base_path` = `/data/ars`
- `semaines_historique` = `12`
- `seuil_alerte_incidence` = `150`
- `seuil_urgence_incidence` = `500`
- `seuil_alerte_zscore` = `1.5`
- `seuil_urgence_zscore` = `3.0`
- `departements_occitanie` = `["09","11","12","30","31","32","34","46","48","65","66","81","82"]`
- `syndromes_surveilles` = `["GRIPPE","GEA","SG","BRONCHIO","COVID19"]`

### 5) Lancement du DAG
- Activer `ars_epidemio_dag`
- Trigger manuel
- Verifier que le run passe en `success`

## Architecture des donnees
- Donnees brutes: `/data/ars/raw/<annee>/<semaine>/sursaud_<semaine>.json`
- Indicateurs: `/data/ars/indicateurs/indicateurs_<semaine>.json`
- Rapports: `/data/ars/rapports/<annee>/<semaine>/rapport_<semaine>.json`

## Decisions techniques
- Utilisation de `CeleryExecutor` pour separer scheduler/worker.
- Utilisation d'un PostgreSQL dedie (`postgres-ars`) pour les donnees metier.
- Idempotence des insertions avec `ON CONFLICT DO UPDATE`.
- DAG rejouable avec `catchup=True` et `max_active_runs=1`.

## Difficultes rencontrees et solutions
- Erreur d'import DAG: suppression de `provide_context` (non necessaire en Airflow 2.8).
- Erreur de droits sur `/data/ars`: correction des permissions du volume Docker.
- Sortie `ls -R` trop longue: creation d'une version echantillon pour la capture.

## Verifications finales
```powershell
docker compose ps
docker compose exec postgres-ars psql -U postgres -d ars_epidemio -c "SELECT COUNT(*) FROM donnees_hebdomadaires;"
docker compose exec airflow-worker bash -lc "ls -R /data/ars"
```
