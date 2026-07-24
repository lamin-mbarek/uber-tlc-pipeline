"""Étape 3 — Transformation des données de trajets avec PySpark.

Lit les fichiers Parquet bruts, applique le nettoyage et les enrichissements
métier, puis écrit le résultat partitionné dans ``data/processed/``.

Règles de transformation prévues :
    - suppression des doublons et des lignes aux champs critiques manquants
    - typage explicite des horodatages et des montants
    - filtrage des courses aberrantes (distance ou durée nulle ou négative)
    - calcul de la durée du trajet et de la vitesse moyenne
    - extraction des dimensions temporelles (année, mois, jour, heure, jour de semaine)

Exécution autonome :
    python -m src.transform.transform
"""


def transform_trips(
    input_dir: str = "data/raw",
    output_dir: str = "data/processed",
) -> str:
    """Nettoie et enrichit les données de trajets TLC via PySpark.

    Args:
        input_dir: Répertoire contenant les fichiers Parquet bruts.
        output_dir: Répertoire d'écriture des données transformées.

    Returns:
        Le chemin du répertoire contenant les données transformées.

    Raises:
        FileNotFoundError: Si aucun fichier Parquet n'est trouvé dans ``input_dir``.
    """
    # TODO : implémenter la transformation
    #   1. Créer la SparkSession (SparkSession.builder.appName("uber-tlc-transform"))
    #   2. Lire les Parquet : spark.read.parquet(f"{input_dir}/*.parquet")
    #   3. Nettoyer :
    #        - dropDuplicates() et dropna() sur les colonnes critiques
    #        - filtrer trip_miles > 0 et durée > 0
    #   4. Enrichir :
    #        - trip_duration_min = (dropoff_datetime - pickup_datetime) / 60
    #        - avg_speed_mph = trip_miles / (trip_duration_min / 60)
    #        - pickup_year / pickup_month / pickup_day / pickup_hour / pickup_dayofweek
    #   5. Écrire en Parquet partitionné par (pickup_year, pickup_month),
    #      en mode "overwrite"
    #   6. Journaliser le nombre de lignes avant / après via src.utils.logger
    #   7. Arrêter la SparkSession et retourner output_dir
    raise NotImplementedError("Fonction à implémenter")


if __name__ == "__main__":
    transform_trips()
