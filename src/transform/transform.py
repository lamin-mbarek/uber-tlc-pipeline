"""Étape 3 — Transformation des données TLC avec PySpark.

Lit les fichiers Parquet bruts, applique un nettoyage et un enrichissement, puis
produit deux sortes de livrables :

- une table de faits nettoyée (un enregistrement = un trajet) ;
- des tables agrégées prêtes pour l'analyse et la visualisation.

Les données brutes ne sont jamais modifiées : les résultats sont écrits dans un
répertoire de sortie distinct (``data/processed`` par défaut).

Exécution autonome :
    python -m src.transform.transform
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Le schéma varie selon le jeu de données TLC (fhv, fhvhv, yellow, green).
# On déclare les noms possibles pour chaque champ logique et on résout
# dynamiquement, en ignorant la casse.
COLONNES_POSSIBLES = {
    "pickup_datetime": ["pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime"],
    "dropoff_datetime": ["dropoff_datetime", "dropOff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"],
    "pickup_location_id": ["pulocationid", "pickup_location_id"],
    "dropoff_location_id": ["dolocationid", "dropoff_location_id"],
}

# Bornes de plausibilité d'un trajet, en minutes.
DUREE_MIN = 1
DUREE_MAX = 300  # 5 heures


def creer_session_spark(nom_app: str = "uber-tlc-pipeline") -> SparkSession:
    """Crée (ou récupère) la session Spark locale."""
    return (
        SparkSession.builder
        .appName(nom_app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def resoudre_colonnes(df: DataFrame) -> dict[str, str]:
    """Associe chaque champ logique au nom réel présent dans le DataFrame.

    Args:
        df: DataFrame brut lu depuis les fichiers Parquet.

    Returns:
        Un dictionnaire {champ_logique: nom_reel_de_colonne}.

    Raises:
        ValueError: Si une colonne indispensable est introuvable.
    """
    # Table de correspondance insensible à la casse.
    reelles = {c.lower(): c for c in df.columns}
    resolues: dict[str, str] = {}

    for champ, candidats in COLONNES_POSSIBLES.items():
        for candidat in candidats:
            if candidat.lower() in reelles:
                resolues[champ] = reelles[candidat.lower()]
                break

    for obligatoire in ("pickup_datetime", "dropoff_datetime"):
        if obligatoire not in resolues:
            raise ValueError(
                f"Colonne '{obligatoire}' introuvable. "
                f"Colonnes disponibles : {df.columns}"
            )

    logger.info("Colonnes résolues : %s", resolues)
    return resolues


def nettoyer_et_enrichir(df: DataFrame, colonnes: dict[str, str]) -> DataFrame:
    """Nettoie les trajets et ajoute les colonnes d'analyse.

    Nettoyage : suppression des doublons, des valeurs manquantes sur les dates,
    et des trajets de durée invalide ou aberrante.

    Enrichissement : durée en minutes, heure, jour de la semaine, indicateur
    week-end, date du trajet.
    """
    pickup = colonnes["pickup_datetime"]
    dropoff = colonnes["dropoff_datetime"]

    lignes_avant = df.count()

    df = (
        df
        .dropDuplicates()
        .filter(F.col(pickup).isNotNull() & F.col(dropoff).isNotNull())
        # Le trajet doit se terminer après avoir commencé.
        .filter(F.col(dropoff) > F.col(pickup))
    )

    df = (
        df
        .withColumn(
            "duree_minutes",
            (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60,
        )
        # Écarte les durées implausibles (capteurs défaillants, saisies erronées).
        .filter(
            (F.col("duree_minutes") >= DUREE_MIN)
            & (F.col("duree_minutes") <= DUREE_MAX)
        )
        .withColumn("date_trajet", F.to_date(F.col(pickup)))
        .withColumn("heure", F.hour(F.col(pickup)))
        # dayofweek : 1 = dimanche ... 7 = samedi
        .withColumn("jour_semaine", F.dayofweek(F.col(pickup)))
        .withColumn("nom_jour", F.date_format(F.col(pickup), "EEEE"))
        .withColumn(
            "est_weekend",
            F.when(F.col("jour_semaine").isin(1, 7), True).otherwise(False),
        )
    )

    lignes_apres = df.count()
    logger.info(
        "Nettoyage : %d lignes en entrée, %d conservées (%.1f %% écartées).",
        lignes_avant,
        lignes_apres,
        100 * (1 - lignes_apres / lignes_avant) if lignes_avant else 0,
    )
    return df


def construire_agregations(df: DataFrame, colonnes: dict[str, str]) -> dict[str, DataFrame]:
    """Produit les tables agrégées répondant aux questions métier."""
    agregations: dict[str, DataFrame] = {}

    # Volume et durée moyenne par heure de la journée.
    agregations["trajets_par_heure"] = (
        df.groupBy("heure")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.avg("duree_minutes"), 2).alias("duree_moyenne_min"),
        )
        .orderBy("heure")
    )

    # Activité par jour de la semaine.
    agregations["trajets_par_jour_semaine"] = (
        df.groupBy("jour_semaine", "nom_jour", "est_weekend")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.avg("duree_minutes"), 2).alias("duree_moyenne_min"),
        )
        .orderBy("jour_semaine")
    )

    # Évolution quotidienne.
    agregations["trajets_par_date"] = (
        df.groupBy("date_trajet")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.avg("duree_minutes"), 2).alias("duree_moyenne_min"),
        )
        .orderBy("date_trajet")
    )

    # Zones de prise en charge les plus fréquentées (si la colonne existe).
    if "pickup_location_id" in colonnes:
        pu = colonnes["pickup_location_id"]
        agregations["zones_depart"] = (
            df.groupBy(F.col(pu).alias("zone_id"))
            .agg(F.count("*").alias("nombre_trajets"))
            .orderBy(F.desc("nombre_trajets"))
        )

    return agregations


def transform_data(
    input_dir: str = "data/raw",
    output_dir: str = "data/processed",
) -> dict[str, str]:
    """Exécute la transformation complète des données TLC.

    Args:
        input_dir: Répertoire contenant les fichiers Parquet bruts.
        output_dir: Répertoire de destination des données transformées.

    Returns:
        Un dictionnaire {nom_de_la_table: chemin_de_sortie}.

    Raises:
        FileNotFoundError: Si aucun fichier Parquet n'est trouvé en entrée.
    """
    source = Path(input_dir)
    fichiers = sorted(source.glob("*.parquet"))
    if not fichiers:
        raise FileNotFoundError(
            f"Aucun fichier Parquet dans {source}. Lancez d'abord l'extraction."
        )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    spark = creer_session_spark()
    # Réduit le bruit dans la console : seuls les avertissements sont affichés.
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Lecture de %d fichier(s) depuis %s", len(fichiers), source)
        df_brut = spark.read.parquet(str(source / "*.parquet"))

        colonnes = resoudre_colonnes(df_brut)
        df_propre = nettoyer_et_enrichir(df_brut, colonnes)

        # Le DataFrame nettoyé est réutilisé par toutes les agrégations :
        # on le met en cache pour éviter de tout recalculer à chaque action.
        df_propre.cache()

        sorties: dict[str, str] = {}

        # 1. Table de faits nettoyée, partitionnée par date pour les requêtes.
        chemin_faits = destination / "trajets_nettoyes"
        (
            df_propre.write
            .mode("overwrite")
            .partitionBy("date_trajet")
            .parquet(str(chemin_faits))
        )
        sorties["trajets_nettoyes"] = str(chemin_faits)
        logger.info("Écrit : %s", chemin_faits)

        # 2. Tables agrégées.
        for nom, df_agrege in construire_agregations(df_propre, colonnes).items():
            chemin = destination / nom
            # coalesce(1) : ces tables sont petites, un seul fichier suffit.
            df_agrege.coalesce(1).write.mode("overwrite").parquet(str(chemin))
            sorties[nom] = str(chemin)
            logger.info("Écrit : %s", chemin)

        df_propre.unpersist()
        logger.info("Transformation terminée : %d table(s) produite(s).", len(sorties))
        return sorties

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_data()