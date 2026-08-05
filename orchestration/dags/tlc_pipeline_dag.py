"""DAG d'orchestration du pipeline TLC.

Enchaîne les quatre étapes du pipeline sous forme de tâches Airflow :
    extract -> load_gcs -> transform -> load_warehouse

Chaque tâche appelle la fonction Python correspondante du package `src`.
Le PYTHONPATH est configuré dans l'image pour rendre `src` importable.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Les fonctions métier du pipeline (déjà écrites et testées).
from src.extract.extract import extract_tlc_data
from src.load_gcs.load_gcs import upload_to_gcs
from src.load_warehouse.load_warehouse import load_to_bigquery
from src.transform.transform import transform_data

default_args = {
    "owner": "lamin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tlc_pipeline",
    description="Pipeline ETL des données de trajets NYC TLC",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # déclenchement manuel pour l'instant
    catchup=False,
    tags=["tlc", "etl", "portfolio"],
) as dag:

    tache_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_tlc_data,
    )

    tache_load_gcs = PythonOperator(
        task_id="load_gcs",
        python_callable=upload_to_gcs,
    )

    tache_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    tache_load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_to_bigquery,
    )

    # Définition de l'ordre d'exécution.
    tache_extract >> tache_load_gcs >> tache_transform >> tache_load_warehouse