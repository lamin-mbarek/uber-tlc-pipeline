# Uber / NYC TLC Trip Data Pipeline

Pipeline ETL de bout en bout qui ingère les données de trajets publiées par la
**NYC Taxi & Limousine Commission** (TLC), les nettoie et les agrège avec PySpark,
puis les expose dans Google BigQuery pour l'analyse et la restitution dans Looker.

---

## Contexte métier

La TLC publie chaque mois, au format Parquet, l'intégralité des courses réalisées
à New York : taxis jaunes, taxis verts, VTC à haut volume (Uber, Lyft) et
véhicules de location. Chaque fichier mensuel contient plusieurs millions de
lignes — volume trop important pour un traitement en mémoire avec pandas seul.

Ce pipeline répond aux questions métier suivantes :

- Quels sont les créneaux horaires et les zones les plus demandés ?
- Comment évolue le revenu moyen par course dans le temps ?
- Quelle est la répartition des distances, durées et pourboires par type de service ?
- Quels axes origine → destination concentrent le plus de volume ?

L'objectif technique est d'obtenir des tables analytiques propres, partitionnées
et requêtables à faible coût dans BigQuery.

---

## Architecture

```
        ┌─────────────────────────────┐
        │   NYC TLC Trip Record Data   │
        │   (fichiers Parquet publics) │
        └──────────────┬───────────────┘
                       │  HTTP (requests)
                       ▼
        ┌─────────────────────────────┐
        │  1. EXTRACT                  │   src/extract/extract.py
        │  Téléchargement mensuel      │   → data/raw/
        │  + retries / validation      │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │  2. LOAD → GCS               │   src/load_gcs/load_gcs.py
        │  Dépôt du brut en data lake  │   → gs://<bucket>/raw/
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │  3. TRANSFORM (PySpark)      │   src/transform/transform.py
        │  Nettoyage, typage, calculs, │   → data/processed/
        │  partitionnement             │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │  4. LOAD → BigQuery          │   src/load_warehouse/load_warehouse.py
        │  Tables partitionnées        │   → <project>.<dataset>.<table>
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │  5. DATAVIZ — Looker         │   dashboards/
        └─────────────────────────────┘

  Orchestration : Apache Airflow (ou Mage)  →  orchestration/pipeline.py
  Exécution     : Docker / docker-compose
```

---

## Stack technique

| Étape           | Technologie                          |
| --------------- | ------------------------------------ |
| Extraction      | Python, `requests`                   |
| Data lake       | Google Cloud Storage                 |
| Transformation  | PySpark                              |
| Data warehouse  | Google BigQuery                      |
| Visualisation   | Looker                               |
| Orchestration   | Apache Airflow (ou Mage)             |
| Conteneurisation| Docker, docker-compose               |
| Configuration   | YAML (`config/config.yaml`), `.env`  |

---

## Installation

### Prérequis

- Python 3.10+
- Java 11 ou 17 (requis par Spark)
- Docker et docker-compose
- Un projet GCP avec un bucket GCS et un dataset BigQuery
- Un compte de service GCP disposant des rôles `Storage Admin` et `BigQuery Data Editor`

### Mise en place locale

```bash
# 1. Cloner le dépôt
git clone <url-du-depot>
cd uber-tlc-pipeline

# 2. Créer et activer un environnement virtuel
python -m venv venv
source venv/bin/activate        # Windows : venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env
# puis éditer .env avec les valeurs du projet GCP

# 5. Déposer la clé du compte de service et pointer GOOGLE_APPLICATION_CREDENTIALS dessus
```

### Configuration

Les paramètres d'extraction (dataset, année, mois à traiter, retries) se règlent
dans [config/config.yaml](config/config.yaml). Les secrets et identifiants GCP
restent dans `.env`, qui n'est jamais versionné.

---

## Exécution

### En local, étape par étape

```bash
python -m src.extract.extract                 # télécharge les Parquet TLC
python -m src.load_gcs.load_gcs               # dépose le brut sur GCS
python -m src.transform.transform             # nettoie et agrège via Spark
python -m src.load_warehouse.load_warehouse   # charge les tables BigQuery
```

### Pipeline complet orchestré

```bash
python orchestration/pipeline.py
```

### Avec Docker

```bash
docker-compose up --build          # démarre Airflow + les services du pipeline
docker-compose down                # arrête et nettoie
```

L'interface Airflow est alors disponible sur http://localhost:8080.

### Tests

```bash
pytest tests/ -v
```

---

## Structure du projet

```
uber-tlc-pipeline/
├── config/            Configuration YAML du pipeline
├── data/raw/          Fichiers Parquet bruts téléchargés (non versionnés)
├── data/processed/    Sorties Spark nettoyées (non versionnées)
├── src/extract/       Téléchargement depuis la source TLC
├── src/load_gcs/      Upload vers le data lake GCS
├── src/transform/     Transformations PySpark
├── src/load_warehouse/Chargement dans BigQuery
├── src/utils/         Utilitaires transverses (logging)
├── sql/               DDL et requêtes analytiques
├── orchestration/     Définition du DAG / pipeline
├── dashboards/        Captures et documentation Looker
├── notebooks/         Exploration ad hoc
└── tests/             Tests unitaires
```

---

## Source des données

NYC TLC Trip Record Data :
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
