"""Orchestration du pipeline uber-tlc-pipeline.

Définit l'enchaînement des quatre étapes ETL sous forme de DAG Airflow :

    extract → load_gcs → transform → load_warehouse

Le fichier est monté dans le dossier ``dags/`` d'Airflow par docker-compose.
Il reste exécutable directement (``python orchestration/pipeline.py``) pour
lancer le pipeline en séquentiel, sans ordonnanceur — pratique en développement.
"""

from datetime import datetime, timedelta

# Paramètres par défaut appliqués à toutes les tâches du DAG.
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,          # un mois en échec ne bloque pas les suivants
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Identifiant et cadence du DAG : les données TLC sont publiées mensuellement.
DAG_ID = "uber_tlc_pipeline"
SCHEDULE = "@monthly"
START_DATE = datetime(2024, 1, 1)


def build_dag():
    """Construit le DAG Airflow orchestrant les quatre étapes du pipeline.

    Returns:
        Le DAG Airflow, prêt à être découvert par l'ordonnanceur.
    """
    # TODO : implémenter la construction du DAG
    #   1. Instancier le DAG (dag_id=DAG_ID, schedule=SCHEDULE,
    #      start_date=START_DATE, catchup=False, default_args=DEFAULT_ARGS)
    #   2. Déclarer une PythonOperator par étape :
    #        - extract_task        -> src.extract.extract:extract_tlc_data
    #        - load_gcs_task       -> src.load_gcs.load_gcs:upload_to_gcs
    #        - transform_task      -> src.transform.transform:transform_trips
    #        - load_warehouse_task -> src.load_warehouse.load_warehouse:load_to_bigquery
    #   3. Chaîner : extract >> load_gcs >> transform >> load_warehouse
    #   4. Retourner le DAG
    raise NotImplementedError("Fonction à implémenter")


def run_pipeline_locally() -> None:
    """Exécute les quatre étapes en séquentiel, sans Airflow.

    Mode de secours pour le développement et le débogage : appelle directement
    les fonctions de chaque module, dans l'ordre, sans ordonnanceur ni retries.
    """
    # TODO : implémenter l'exécution séquentielle
    #   1. Importer les fonctions des quatre modules de src/
    #   2. Les appeler dans l'ordre en propageant les chemins produits
    #   3. Journaliser le début et la fin de chaque étape via src.utils.logger
    raise NotImplementedError("Fonction à implémenter")


# Airflow découvre le DAG en cherchant un objet DAG au niveau du module.
# TODO : décommenter une fois build_dag() implémentée.
# dag = build_dag()


if __name__ == "__main__":
    run_pipeline_locally()
