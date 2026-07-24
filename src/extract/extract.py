import time
from pathlib import Path

import requests
import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_tlc_data(config_path: str = "config/config.yaml") -> list[str]:
    """Télécharge les fichiers Parquet TLC décrits par la configuration.

    Pour chaque mois listé dans ``extraction.months``, construit l'URL du fichier
    Parquet correspondant, le télécharge avec une politique de retries, puis
    l'enregistre dans le répertoire de sortie.

    Args:
        config_path: Chemin vers le fichier de configuration YAML.

    Returns:
        La liste des chemins locaux des fichiers effectivement téléchargés.

    Raises:
        RuntimeError: Si un fichier reste inaccessible après ``max_retries``
            tentatives.
    """
    # 1. Charger la configuration et isoler  la section "extraction".
    with open(config_path, "r", encoding="utf-8") as fichier:
        config = yaml.safe_load(fichier)
    extraction = config["extraction"]

    dataset = extraction["dataset"]
    base_url = extraction["base_url"].rstrip("/")
    year = extraction["year"]
    months = extraction["months"]
    output_dir = Path(extraction["output_dir"])
    max_retries = extraction["max_retries"]
    retry_delay = extraction["retry_delay"]

    # 2. S'assurer que le répertoire de sortie existe.
    output_dir.mkdir(parents=True, exist_ok=True)

    chemins_telecharges: list[str] = []

    # 3. Traiter chaque mois demandé.
    for month in months:
        nom_fichier = f"{dataset}_{year}-{month}.parquet"
        url = f"{base_url}/{nom_fichier}"
        chemin_local = output_dir / nom_fichier

        # Idempotence : ne pas re-télécharger un fichier déjà présent.
        if chemin_local.exists():
            logger.info("Déjà présent, ignoré : %s", chemin_local)
            chemins_telecharges.append(str(chemin_local))
            continue

        # Téléchargement avec politique de retries.
        for tentative in range(1, max_retries + 1):
            try:
                logger.info("Téléchargement (%d/%d) : %s", tentative, max_retries, url)
                with requests.get(url, stream=True, timeout=60) as reponse:
                    reponse.raise_for_status()
                    # Écriture en streaming pour ne pas charger le fichier en RAM.
                    with open(chemin_local, "wb") as sortie:
                        for bloc in reponse.iter_content(chunk_size=8192):
                            sortie.write(bloc)
                logger.info("Enregistré : %s", chemin_local)
                chemins_telecharges.append(str(chemin_local))
                break
            except requests.RequestException as erreur:
                logger.warning("Échec de la tentative %d : %s", tentative, erreur)
                # Nettoyer un fichier partiel avant de réessayer.
                chemin_local.unlink(missing_ok=True)
                if tentative < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Impossible de télécharger {url} après {max_retries} tentatives"
                    ) from erreur

    logger.info("Extraction terminée : %d fichier(s).", len(chemins_telecharges))
    return chemins_telecharges


if __name__ == "__main__":
    extract_tlc_data()
 