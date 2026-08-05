"""Étape 4 — Chargement des tables Parquet de GCS vers Google BigQuery.

Lit les tables produites par la transformation dans
``gs://{GCS_BUCKET}/processed/{table}/`` et les charge dans le dataset BigQuery
cible (``BQ_DATASET``, par défaut ``tlc_analytics``). Chaque table est chargée
en mode ``WRITE_TRUNCATE`` : le module est donc idempotent (chaque exécution
remplace intégralement la table).

Authentification : Application Default Credentials (ADC), aucune clé JSON.

Exécution autonome :
    python -m src.load_warehouse.load_warehouse
"""

import os

from dotenv import load_dotenv
from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Préfixe des données transformées dans le bucket.
PROCESSED_PREFIX = "processed"

# Tables à charger dans BigQuery (paramétrable : ajouter/retirer ici).
# L'ordre n'a pas d'importance, chaque chargement est indépendant.
TABLES = [
    "trajets_nettoyes",
    "kpi_globaux",
    "kpi_par_plateforme",
    "trajets_par_heure",
    "trajets_par_jour_semaine",
    "trajets_par_annee_mois",
    "revenu_par_zone_depart",
]

# Tables écrites par Spark avec un partitionnement de type Hive
# (sous-dossiers `colonne=valeur/`). La colonne de partition n'existe alors que
# dans le chemin : BigQuery doit la reconstruire via le partitionnement Hive.
TABLES_PARTITIONNEES = {
    "trajets_nettoyes": "date_trajet",
}

# Région du dataset BigQuery. Le job de chargement doit s'exécuter dans la même
# région que le dataset, sous peine d'erreur « dataset not found in location ».
DEFAULT_LOCATION = "us-central1"


def _config_chargement(table: str, bucket: str) -> tuple[str, bigquery.LoadJobConfig]:
    """Construit l'URI source et la configuration du job pour une table donnée.

    Args:
        table: Nom de la table (= nom du sous-dossier dans ``processed/``).
        bucket: Nom du bucket GCS.

    Returns:
        Un couple ``(source_uri, job_config)`` prêt pour ``load_table_from_uri``.
    """
    base = f"gs://{bucket}/{PROCESSED_PREFIX}/{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    # Le motif `*.parquet` capture les fichiers à n'importe quelle profondeur
    # (y compris dans les sous-dossiers de partition) tout en excluant les
    # marqueurs Spark tels que `_SUCCESS`.
    source_uri = f"{base}/*.parquet"

    # Pour une table partitionnée façon Hive, on active la détection du
    # partitionnement afin de récupérer la colonne encodée dans le chemin.
    if table in TABLES_PARTITIONNEES:
        options = bigquery.HivePartitioningOptions()
        options.mode = "AUTO"                 # types des clés déduits automatiquement
        options.source_uri_prefix = base      # préfixe précédant `colonne=valeur/`
        job_config.hive_partitioning = options

    return source_uri, job_config


def load_to_bigquery() -> dict[str, int]:
    """Charge toutes les tables de ``processed/`` (GCS) dans BigQuery.

    Le dataset cible est supposé déjà créé (nom lu depuis ``BQ_DATASET``, défaut
    ``tlc_analytics``). Chaque table est chargée indépendamment ; une table
    absente dans GCS est journalisée puis ignorée, sans interrompre les autres.

    Returns:
        Un dictionnaire ``{nom_table: nombre_de_lignes_chargées}``. Les tables
        ignorées (absentes ou en erreur) n'y figurent pas.

    Raises:
        EnvironmentError: Si la variable ``GCS_BUCKET`` n'est pas définie.
    """
    load_dotenv()

    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise EnvironmentError(
            "Variable GCS_BUCKET non définie. Renseignez-la dans le fichier .env."
        )

    dataset = os.getenv("BQ_DATASET", "tlc_analytics")
    location = os.getenv("BQ_LOCATION", DEFAULT_LOCATION)
    # GCP_PROJECT est facultatif : l'ADC fournit déjà un projet par défaut.
    project = os.getenv("GCP_PROJECT")

    client = bigquery.Client(project=project) if project else bigquery.Client()
    logger.info(
        "Chargement vers BigQuery : projet=%s, dataset=%s, région=%s",
        client.project, dataset, location,
    )

    resultats: dict[str, int] = {}

    for table in TABLES:
        table_id = f"{client.project}.{dataset}.{table}"
        source_uri, job_config = _config_chargement(table, bucket)

        try:
            logger.info("Chargement de %s depuis %s", table, source_uri)
            job = client.load_table_from_uri(
                source_uri, table_id, job_config=job_config, location=location
            )
            job.result()  # attend la fin du job (lève une exception en cas d'échec)

            lignes = job.output_rows
            resultats[table] = lignes
            logger.info("Table %s chargée : %d ligne(s).", table, lignes)

        except gcp_exceptions.NotFound:
            # Aucun fichier trouvé dans GCS pour cette table : on continue.
            logger.warning("Table ignorée (introuvable dans GCS) : %s", table)
        except gcp_exceptions.GoogleAPICallError as erreur:
            # Autre erreur BigQuery/GCS : journalisée, sans faire échouer le reste.
            logger.error("Échec du chargement de %s : %s", table, erreur)

    logger.info("Chargement terminé : %d table(s) chargée(s).", len(resultats))
    return resultats


if __name__ == "__main__":
    load_to_bigquery()
