"""Package racine du pipeline uber-tlc-pipeline.

Regroupe les quatre étapes ETL du pipeline ainsi que les utilitaires partagés :

- ``extract``        : téléchargement des fichiers Parquet depuis la source NYC TLC
- ``load_gcs``       : dépôt des fichiers bruts dans le data lake Google Cloud Storage
- ``transform``      : nettoyage et agrégation des données avec PySpark
- ``load_warehouse`` : chargement des tables analytiques dans Google BigQuery
- ``utils``          : fonctions transverses (configuration du logging, etc.)
"""
