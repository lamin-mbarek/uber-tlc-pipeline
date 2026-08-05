"""Étape 3 — Transformation des données fhvhv lues depuis GCS.

Spark lit les fichiers Parquet bruts (VTC à haut volume : Uber, Lyft, Via, Juno)
directement depuis le bucket Google Cloud Storage (``gs://``), applique un
nettoyage et un enrichissement métier, puis réécrit plusieurs tables agrégées
dans GCS sous le préfixe ``processed`` — prêtes pour BigQuery puis Power BI.

L'axe central de l'analyse est la comparaison **Uber vs Lyft** (colonne
``plateforme``), présente dans la plupart des agrégations.

Prérequis :
- Connecteur Hadoop-GCS (chargé automatiquement via spark.jars.packages).
- Identifiants ADC accessibles (variable GOOGLE_APPLICATION_CREDENTIALS).
- Variable GCS_BUCKET définie dans le fichier .env.
"""

import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, IntegerType, ShortType

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Connecteur GCS compatible Spark 3.5 / Hadoop 3.
GCS_CONNECTOR = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21"

# Correspondance code de licence TLC -> nom lisible de plateforme.
PLATEFORMES = {
    "HV0002": "Juno",
    "HV0003": "Uber",
    "HV0004": "Via",
    "HV0005": "Lyft",
}

# Bornes de plausibilité d'un trajet.
TRIP_TIME_MIN_S = 60      # 1 minute
TRIP_TIME_MAX_S = 18000   # 5 heures
TRIP_MILES_MAX = 200      # miles


def creer_session_spark(nom_app: str = "uber-tlc-pipeline") -> SparkSession:
    """Crée une session Spark configurée pour lire et écrire sur GCS via ADC.

    Le connecteur GCS est récupéré depuis Maven au premier démarrage, puis mis
    en cache. L'authentification s'appuie sur les Application Default
    Credentials, sans clé de compte de service.
    """
    # Dans le conteneur, le chemin est fourni par GOOGLE_APPLICATION_CREDENTIALS.
    # Le repli couvre une exécution locale sous Windows.
    adc_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join(
            os.environ.get("APPDATA", ""), "gcloud",
            "application_default_credentials.json",
        ),
    )

    builder = (
        SparkSession.builder
        .appName(nom_app)
        .config("spark.jars.packages", GCS_CONNECTOR)
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        .config("spark.sql.session.timeZone", "UTC")
        # Charge en priorité les classes du connecteur : évite le conflit Guava.
        .config("spark.driver.userClassPathFirst", "true")
        .config("spark.executor.userClassPathFirst", "true")
        # Volume réduit : 200 partitions par défaut seraient contre-productives.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )

    if os.path.exists(adc_path):
        builder = builder.config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", adc_path
        )
        logger.info("Identifiants ADC détectés : %s", adc_path)
    else:
        logger.warning(
            "Fichier ADC introuvable (%s). Lancez "
            "'gcloud auth application-default login'.", adc_path
        )

    return builder.getOrCreate()


def lire_et_unifier(spark: SparkSession, source_glob: str) -> DataFrame:
    """Lit tous les Parquet correspondant au motif et harmonise leurs schémas.

    ``spark.read.option("mergeSchema", ...)`` échoue ici : d'un mois à l'autre,
    les colonnes de zones (PULocationID / DOLocationID) sont tantôt ``INT``,
    tantôt ``BIGINT``, et Spark refuse de fusionner ces deux types. On lit donc
    chaque fichier séparément, on élargit tout entier 32 bits en ``long``, puis
    on empile les DataFrames avec ``unionByName`` (tolérant aux colonnes
    manquantes). Le volume est négligeable : les fichiers sont échantillonnés.

    Args:
        spark: Session Spark active.
        source_glob: Motif GCS des fichiers à lire (``gs://bucket/raw/*.parquet``).

    Returns:
        Un unique DataFrame au schéma homogène.

    Raises:
        FileNotFoundError: Si aucun fichier ne correspond au motif.
    """
    # Résolution du motif via le système de fichiers Hadoop (compatible gs://).
    jvm = spark.sparkContext._jvm
    chemin_hadoop = jvm.org.apache.hadoop.fs.Path(source_glob)
    fs = chemin_hadoop.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    statuts = fs.globStatus(chemin_hadoop)
    chemins = [statut.getPath().toString() for statut in statuts]

    if not chemins:
        raise FileNotFoundError(f"Aucun fichier Parquet trouvé pour {source_glob}")

    logger.info("%d fichier(s) à lire et unifier.", len(chemins))

    # Types entiers à élargir en long pour éviter tout conflit INT/BIGINT.
    types_a_elargir = (ByteType, ShortType, IntegerType)

    dataframes: list[DataFrame] = []
    for chemin in chemins:
        df = spark.read.parquet(chemin)
        for champ in df.schema.fields:
            if isinstance(champ.dataType, types_a_elargir):
                df = df.withColumn(champ.name, F.col(champ.name).cast("long"))
        dataframes.append(df)

    # Empilement tolérant : les colonnes absentes d'un fichier sont complétées.
    df_unifie = dataframes[0]
    for df in dataframes[1:]:
        df_unifie = df_unifie.unionByName(df, allowMissingColumns=True)

    return df_unifie


def nettoyer_et_enrichir(df: DataFrame) -> DataFrame:
    """Nettoie les trajets fhvhv et calcule les colonnes d'analyse.

    Nettoyage :
        - suppression des doublons ;
        - lignes sans pickup/dropoff écartées ;
        - dropoff strictement postérieur au pickup ;
        - trip_time compris entre 60 et 18000 s (1 min à 5 h) ;
        - trip_miles strictement positif et inférieur à 200 ;
        - base_passenger_fare positif ou nul.

    Enrichissement : plateforme lisible, durée, temps d'attente, vitesse,
    revenu total, taux de reversement au chauffeur, indicateurs booléens
    (pourboire, course partagée, PMR) et dimensions temporelles.

    Args:
        df: DataFrame brut fhvhv unifié.

    Returns:
        Le DataFrame nettoyé et enrichi.
    """
    lignes_avant = df.count()

    # --- Nettoyage ---
    df = (
        df
        .dropDuplicates()
        .filter(F.col("pickup_datetime").isNotNull()
                & F.col("dropoff_datetime").isNotNull())
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter((F.col("trip_time") >= TRIP_TIME_MIN_S)
                & (F.col("trip_time") <= TRIP_TIME_MAX_S))
        .filter((F.col("trip_miles") > 0) & (F.col("trip_miles") < TRIP_MILES_MAX))
        .filter(F.col("base_passenger_fare") >= 0)
    )

    # --- Plateforme lisible (Uber / Lyft / Via / Juno / Autre) ---
    plateforme = F.lit("Autre")
    for code, nom in PLATEFORMES.items():
        plateforme = F.when(F.col("hvfhs_license_num") == code, nom).otherwise(plateforme)
    df = df.withColumn("plateforme", plateforme)

    # --- Colonnes dérivées ---
    # Temps d'attente : null si request_datetime manquant ou écart négatif.
    ecart_attente_s = (
        F.unix_timestamp("pickup_datetime") - F.unix_timestamp("request_datetime")
    )
    df = (
        df
        .withColumn("duree_minutes", F.round(F.col("trip_time") / 60, 2))
        .withColumn(
            "temps_attente_min",
            F.round(
                F.when(
                    F.col("request_datetime").isNotNull() & (ecart_attente_s >= 0),
                    ecart_attente_s / 60,
                ),
                2,
            ),
        )
        # Protection contre la division par zéro (trip_time >= 60 après filtrage).
        .withColumn(
            "vitesse_mph",
            F.when(
                F.col("trip_time") > 0,
                F.round(F.col("trip_miles") / (F.col("trip_time") / 3600), 2),
            ),
        )
        .withColumn(
            "revenu_total",
            F.round(
                F.col("base_passenger_fare")
                + F.coalesce(F.col("tolls"), F.lit(0.0))
                + F.coalesce(F.col("congestion_surcharge"), F.lit(0.0))
                + F.coalesce(F.col("airport_fee"), F.lit(0.0))
                + F.coalesce(F.col("tips"), F.lit(0.0)),
                2,
            ),
        )
        # Taux de reversement : null si le tarif de base est nul.
        .withColumn(
            "taux_reversement",
            F.when(
                F.col("base_passenger_fare") != 0,
                F.round(F.col("driver_pay") / F.col("base_passenger_fare"), 2),
            ),
        )
        .withColumn("a_pourboire", F.coalesce(F.col("tips"), F.lit(0.0)) > 0)
        .withColumn("est_partagee",
                    F.coalesce(F.col("shared_match_flag") == "Y", F.lit(False)))
        .withColumn("est_pmr",
                    F.coalesce(F.col("wav_request_flag") == "Y", F.lit(False)))
        # --- Dimensions temporelles (depuis pickup_datetime) ---
        .withColumn("date_trajet", F.to_date("pickup_datetime"))
        .withColumn("annee", F.year("pickup_datetime"))
        .withColumn("mois", F.month("pickup_datetime"))
        .withColumn("heure", F.hour("pickup_datetime"))
        # dayofweek : 1 = dimanche ... 7 = samedi
        .withColumn("jour_semaine", F.dayofweek("pickup_datetime"))
        .withColumn("nom_jour", F.date_format("pickup_datetime", "EEEE"))
        .withColumn("est_weekend",
                    F.when(F.col("jour_semaine").isin(1, 7), True).otherwise(False))
    )

    lignes_apres = df.count()
    logger.info(
        "Nettoyage : %d lignes en entrée, %d conservées (%.1f %% écartées).",
        lignes_avant, lignes_apres,
        100 * (1 - lignes_apres / lignes_avant) if lignes_avant else 0,
    )
    return df


def _expressions_kpi() -> list:
    """Retourne les expressions d'agrégation des indicateurs clés.

    Mutualisées entre ``kpi_globaux`` (sans regroupement) et
    ``kpi_par_plateforme`` (regroupées par plateforme) pour garantir des
    définitions strictement identiques.
    """
    return [
        F.count("*").alias("nombre_trajets"),
        F.round(F.sum("revenu_total"), 2).alias("revenu_total"),
        F.round(F.avg("revenu_total"), 2).alias("revenu_moyen"),
        F.round(F.avg("tips"), 2).alias("pourboire_moyen"),
        F.round(F.avg("taux_reversement"), 2).alias("taux_reversement_moyen"),
        F.round(F.avg("trip_miles"), 2).alias("distance_moyenne"),
        F.round(F.avg("duree_minutes"), 2).alias("duree_moyenne_min"),
        F.round(F.avg("temps_attente_min"), 2).alias("temps_attente_moyen_min"),
        F.round(F.avg("vitesse_mph"), 2).alias("vitesse_moyenne_mph"),
        F.round(100 * F.avg(F.col("est_partagee").cast("int")), 2).alias("part_partagee"),
        F.round(100 * F.avg(F.col("est_pmr").cast("int")), 2).alias("part_pmr"),
    ]


def construire_agregations(df: DataFrame) -> dict[str, DataFrame]:
    """Produit les tables agrégées alimentant le dashboard Power BI.

    Args:
        df: DataFrame nettoyé et enrichi.

    Returns:
        Un dictionnaire {nom_de_la_table: DataFrame agrégé}.
    """
    agregations: dict[str, DataFrame] = {}

    # Indicateurs clés globaux : une seule ligne.
    agregations["kpi_globaux"] = df.agg(*_expressions_kpi())

    # Table centrale de comparaison : les mêmes indicateurs par plateforme.
    agregations["kpi_par_plateforme"] = (
        df.groupBy("plateforme")
        .agg(*_expressions_kpi())
        .orderBy(F.desc("nombre_trajets"))
    )

    # Activité horaire par plateforme.
    agregations["trajets_par_heure"] = (
        df.groupBy("heure", "plateforme")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.avg("revenu_total"), 2).alias("revenu_moyen"),
            F.round(F.avg("duree_minutes"), 2).alias("duree_moyenne_min"),
            F.round(F.avg("temps_attente_min"), 2).alias("temps_attente_moyen_min"),
        )
        .orderBy("heure", "plateforme")
    )

    # Activité par jour de la semaine et plateforme.
    agregations["trajets_par_jour_semaine"] = (
        df.groupBy("jour_semaine", "nom_jour", "est_weekend", "plateforme")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.avg("revenu_total"), 2).alias("revenu_moyen"),
        )
        .orderBy("jour_semaine", "plateforme")
    )

    # Évolution pluriannuelle par plateforme.
    agregations["trajets_par_annee_mois"] = (
        df.groupBy("annee", "mois", "plateforme")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.sum("revenu_total"), 2).alias("revenu_total"),
            F.round(F.avg("revenu_total"), 2).alias("revenu_moyen"),
            F.round(F.avg("trip_miles"), 2).alias("distance_moyenne"),
        )
        .orderBy("annee", "mois", "plateforme")
    )

    # Revenu par zone de départ et plateforme.
    agregations["revenu_par_zone_depart"] = (
        df.groupBy(F.col("PULocationID").alias("zone_id"), "plateforme")
        .agg(
            F.count("*").alias("nombre_trajets"),
            F.round(F.sum("revenu_total"), 2).alias("revenu_total"),
            F.round(F.avg("revenu_total"), 2).alias("revenu_moyen"),
            F.round(F.avg("trip_miles"), 2).alias("distance_moyenne"),
        )
        .orderBy(F.desc("nombre_trajets"))
    )

    return agregations


# Sous-ensemble de colonnes conservé dans la table de faits (évite une table
# trop large, tout en gardant l'essentiel pour l'analyse fine).
COLONNES_FAITS = [
    "plateforme",
    "PULocationID", "DOLocationID",
    "trip_miles", "duree_minutes", "temps_attente_min", "vitesse_mph",
    "base_passenger_fare", "revenu_total", "tips", "driver_pay", "taux_reversement",
    "a_pourboire", "est_partagee", "est_pmr",
    # Dimensions temporelles (date_trajet sert de clé de partition).
    "date_trajet", "annee", "mois", "heure", "jour_semaine", "nom_jour", "est_weekend",
]


def transform_data(
    gcs_prefix_input: str = "raw",
    gcs_prefix_output: str = "processed",
) -> dict[str, str]:
    """Transforme les données fhvhv lues depuis GCS et réécrit les tables dans GCS.

    Args:
        gcs_prefix_input: Préfixe des fichiers bruts dans le bucket.
        gcs_prefix_output: Préfixe de destination des données transformées.

    Returns:
        Un dictionnaire {nom_de_la_table: URI GCS de sortie}.

    Raises:
        EnvironmentError: Si la variable GCS_BUCKET n'est pas définie.
    """
    load_dotenv()
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise EnvironmentError("Variable GCS_BUCKET non définie dans le fichier .env.")

    source_uri = f"gs://{bucket}/{gcs_prefix_input.strip('/')}/*.parquet"
    base_sortie = f"gs://{bucket}/{gcs_prefix_output.strip('/')}"

    spark = creer_session_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Lecture depuis %s", source_uri)
        # Lecture fichier par fichier avec harmonisation des types : les schémas
        # TLC varient entre 2022 et 2024 (INT vs BIGINT sur les zones), ce que
        # mergeSchema ne sait pas réconcilier.
        df_brut = lire_et_unifier(spark, source_uri)
        df_propre = nettoyer_et_enrichir(df_brut)

        # Le DataFrame nettoyé alimente toutes les agrégations : on le met en
        # cache pour éviter de recalculer le pipeline à chaque action.
        df_propre.cache()

        sorties: dict[str, str] = {}

        # 1. Table de faits nettoyée, partitionnée par date.
        chemin_faits = f"{base_sortie}/trajets_nettoyes"
        (
            df_propre.select(*COLONNES_FAITS).write
            .mode("overwrite")
            .partitionBy("date_trajet")
            .parquet(chemin_faits)
        )
        sorties["trajets_nettoyes"] = chemin_faits
        logger.info("Écrit : %s", chemin_faits)

        # 2. Tables agrégées (petites : un seul fichier par table).
        for nom, df_agrege in construire_agregations(df_propre).items():
            chemin = f"{base_sortie}/{nom}"
            df_agrege.coalesce(1).write.mode("overwrite").parquet(chemin)
            sorties[nom] = chemin
            logger.info("Écrit : %s", chemin)

        df_propre.unpersist()
        logger.info("Transformation terminée : %d table(s) produite(s).", len(sorties))
        return sorties

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_data()
