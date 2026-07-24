"""Étape 4 — Chargement des données transformées dans Google BigQuery.

Charge les Parquet nettoyés produits par l'étape de transformation dans le
dataset BigQuery cible (``BQ_DATASET``), sous forme de tables partitionnées par
date de prise en charge afin de limiter le coût des requêtes analytiques.

Exécution autonome :
    python -m src.load_warehouse.load_warehouse
"""


def load_to_bigquery(
    source_dir: str = "data/processed",
    table_name: str = "fact_trips",
    write_disposition: str = "WRITE_TRUNCATE",
) -> str:
    """Charge les données transformées dans une table BigQuery.

    Args:
        source_dir: Répertoire (ou préfixe GCS) contenant les Parquet transformés.
        table_name: Nom de la table cible dans le dataset ``BQ_DATASET``.
        write_disposition: Comportement d'écriture BigQuery.
            ``WRITE_TRUNCATE`` remplace la table, ``WRITE_APPEND`` ajoute les lignes.

    Returns:
        L'identifiant complet de la table chargée
        (``projet.dataset.table``).

    Raises:
        EnvironmentError: Si ``GCP_PROJECT`` ou ``BQ_DATASET`` ne sont pas définis.
        google.api_core.exceptions.GoogleAPIError: Si le job de chargement échoue.
    """
    # TODO : implémenter le chargement BigQuery
    #   1. Lire GCP_PROJECT et BQ_DATASET depuis l'environnement (.env)
    #   2. Instancier le client : bigquery.Client(project=GCP_PROJECT)
    #   3. Créer le dataset s'il n'existe pas (exists_ok=True)
    #   4. Configurer le job :
    #        - source_format = bigquery.SourceFormat.PARQUET
    #        - write_disposition = paramètre reçu
    #        - time_partitioning sur la colonne de date de prise en charge
    #        - clustering_fields sur les zones (PULocationID, DOLocationID)
    #   5. Lancer load_table_from_uri (source GCS) ou load_table_from_file (local)
    #   6. Attendre la fin du job (job.result()) et journaliser le nombre de lignes
    #   7. Retourner l'identifiant complet de la table
    raise NotImplementedError("Fonction à implémenter")


if __name__ == "__main__":
    load_to_bigquery()
