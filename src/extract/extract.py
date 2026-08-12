"""Étape 1 — Extraction échantillonnée des fichiers Parquet de la NYC TLC.

Pour chaque couple (année, mois) défini dans la configuration, seules les
``sample_rows`` premières lignes du fichier distant sont lues, puis écrites dans
un petit Parquet local sous ``data/raw/``.

Exigence critique : un fichier Parquet TLC pèse plusieurs centaines de Mo. Il
n'est JAMAIS téléchargé en entier. La lecture s'appuie sur ``pyarrow`` + ``fsspec``
et ``ParquetFile(...).iter_batches(batch_size=...)`` : seuls les métadonnées et le
premier lot de lignes transitent par le réseau (requêtes HTTP Range). Aucun
fichier temporaire n'est créé dans ``data/raw``.

Exécution autonome :
    python -m src.extract.extract
"""

import time
from pathlib import Path

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Racine du projet, calculée à partir de l'emplacement de ce fichier :
# .../src/extract/extract.py -> parents[2] == racine du projet.
# Aucun chemin absolu Windows n'est codé en dur.
RACINE_PROJET = Path(__file__).resolve().parents[2]
CONFIG_PAR_DEFAUT = RACINE_PROJET / "config" / "config.yaml"


def lire_echantillon_distant(url: str, nb_lignes: int) -> pa.Table:
    """Lit les ``nb_lignes`` premières lignes d'un Parquet distant sans le rapatrier.

    Ouvre le fichier distant via fsspec (HTTP Range) puis récupère uniquement le
    premier lot renvoyé par ``iter_batches`` : la lecture s'arrête dès que
    ``nb_lignes`` lignes sont disponibles, sans jamais charger le fichier complet.

    Args:
        url: URL du fichier Parquet distant.
        nb_lignes: Nombre de lignes à conserver dans l'échantillon.

    Returns:
        Une table PyArrow contenant au plus ``nb_lignes`` lignes.

    Raises:
        StopIteration: Si le fichier ne contient aucun lot (fichier vide).
    """
    # fsspec gère l'ouverture distante ; le bloc `with` referme le flux réseau.
    with fsspec.open(url, "rb") as flux_distant:
        parquet = pq.ParquetFile(flux_distant)
        # iter_batches ne lit que les groupes de lignes nécessaires pour remplir
        # un lot de `nb_lignes`. On prend le premier lot et on s'arrête là.
        premier_lot = next(parquet.iter_batches(batch_size=nb_lignes))

    # Reconstruit une table à partir de l'unique lot lu, tronquée par sécurité.
    return pa.Table.from_batches([premier_lot]).slice(0, nb_lignes)


def extract_tlc_data(config_path: str | Path = CONFIG_PAR_DEFAUT) -> list[str]:
    """Extrait un échantillon de chaque fichier TLC décrit par la configuration.

    Parcourt chaque couple (année, mois), lit à distance les premières lignes et
    les écrit dans ``data/raw/`` sous le nom du fichier source. Idempotent : un
    échantillon déjà présent n'est pas régénéré. Un fichier inaccessible est
    journalisé puis ignoré, sans interrompre le reste de l'extraction.

    Args:
        config_path: Chemin vers le fichier de configuration YAML.

    Returns:
        La liste des chemins locaux des échantillons Parquet produits.
    """
    with open(config_path, "r", encoding="utf-8") as fichier:
        extraction = yaml.safe_load(fichier)["extraction"]

    dataset = extraction["dataset"]
    base_url = extraction["base_url"].rstrip("/")
    years = extraction["years"]
    months = extraction["months"]
    nb_lignes = extraction.get("sample_rows", 10)
    max_retries = extraction.get("max_retries", 3)
    retry_delay = extraction.get("retry_delay", 5)

    output_dir = RACINE_PROJET / extraction["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    chemins: list[str] = []

    for year in years:
        for month in months:
            nom = f"{dataset}_{year}-{month}.parquet"
            url = f"{base_url}/{nom}"
            destination = output_dir / nom

            # Idempotence : l'échantillon existe déjà, on le réutilise.
            if destination.exists():
                logger.info("Échantillon déjà présent, ignoré : %s", nom)
                chemins.append(str(destination))
                continue

            table = None
            for tentative in range(1, max_retries + 1):
                try:
                    logger.info(
                        "Lecture distante (%d/%d) de %s", tentative, max_retries, nom
                    )
                    table = lire_echantillon_distant(url, nb_lignes)
                    break
                except Exception as erreur:  # réseau, throttling, Parquet vide…
                    # Backoff exponentiel : la source (CloudFront) limite le débit
                    # après plusieurs lectures rapprochées. On attend de plus en
                    # plus longtemps avant de réessayer.
                    logger.warning("Échec tentative %d pour %s : %s", tentative, nom, erreur)
                    if tentative < max_retries:
                        time.sleep(retry_delay * (2 ** (tentative - 1)))

            # Un fichier réellement inaccessible ne doit pas interrompre le pipeline.
            if table is None:
                logger.error("Fichier ignoré (inaccessible) : %s", nom)
                continue

            # Écriture directe à la destination finale : aucun .tmp intermédiaire.
            pq.write_table(table, destination)
            logger.info("Échantillon écrit : %s (%d lignes)", nom, table.num_rows)
            chemins.append(str(destination))

            # Pause de politesse entre deux fichiers pour éviter le throttling.
            time.sleep(1)

    logger.info("Extraction terminée : %d échantillon(s).", len(chemins))
    return chemins


if __name__ == "__main__":
    extract_tlc_data()
