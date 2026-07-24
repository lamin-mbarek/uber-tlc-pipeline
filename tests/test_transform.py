"""Tests unitaires de l'étape de transformation PySpark.

Exécution :
    pytest tests/ -v

Les tests s'appuient sur une SparkSession locale et sur de petits DataFrames
construits à la main, afin de rester rapides et indépendants des données réelles.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Fournit une SparkSession locale partagée par tous les tests.

    Portée ``session`` : le démarrage de Spark coûte plusieurs secondes, la même
    session est donc réutilisée pour l'ensemble du module.

    Yields:
        La SparkSession configurée en mode local.
    """
    # TODO : implémenter la fixture
    #   1. Construire la session :
    #        SparkSession.builder.master("local[2]").appName("tests").getOrCreate()
    #   2. yield la session
    #   3. L'arrêter (spark.stop()) après les tests
    raise NotImplementedError("Fixture à implémenter")


@pytest.fixture
def sample_trips(spark):
    """Construit un jeu de données de trajets minimal couvrant les cas limites.

    Contient volontairement : une course valide, un doublon, une distance nulle,
    une durée négative et une ligne aux champs critiques manquants.

    Args:
        spark: La SparkSession fournie par la fixture ``spark``.

    Returns:
        Un DataFrame Spark reproduisant le schéma des données TLC brutes.
    """
    # TODO : implémenter la fixture
    #   1. Définir un StructType proche du schéma TLC réel
    #   2. Créer les lignes couvrant les cas listés ci-dessus
    #   3. Retourner spark.createDataFrame(rows, schema)
    raise NotImplementedError("Fixture à implémenter")


def test_supprime_les_doublons(sample_trips):
    """Vérifie que les courses dupliquées sont éliminées."""
    # TODO : transformer sample_trips et vérifier que le doublon a disparu
    raise NotImplementedError("Test à implémenter")


def test_filtre_les_distances_invalides(sample_trips):
    """Vérifie que les courses de distance nulle ou négative sont écartées."""
    # TODO : vérifier qu'aucune ligne du résultat n'a trip_miles <= 0
    raise NotImplementedError("Test à implémenter")


def test_filtre_les_durees_invalides(sample_trips):
    """Vérifie que les courses de durée nulle ou négative sont écartées."""
    # TODO : vérifier qu'aucune ligne du résultat n'a trip_duration_min <= 0
    raise NotImplementedError("Test à implémenter")


def test_calcule_la_duree_du_trajet(sample_trips):
    """Vérifie que la durée est calculée correctement à partir des horodatages."""
    # TODO : vérifier trip_duration_min sur une course de référence
    raise NotImplementedError("Test à implémenter")


def test_extrait_les_dimensions_temporelles(sample_trips):
    """Vérifie la présence et la cohérence des colonnes temporelles dérivées."""
    # TODO : vérifier que pickup_year/month/day/hour/dayofweek existent et que
    #        pickup_hour est bien compris entre 0 et 23
    raise NotImplementedError("Test à implémenter")
