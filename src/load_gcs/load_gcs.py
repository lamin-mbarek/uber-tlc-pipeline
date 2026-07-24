

import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

from src.utils.logger import get_logger

logger = get_logger(__name__)


def upload_to_gcs(local_dir: str = "data/raw", gcs_prefix: str = "raw") -> list[str]:
    """Téléverse les fichiers Parquet locaux vers le bucket GCS du projet.

    Args:
        local_dir: Répertoire local contenant les fichiers Parquet bruts.
        gcs_prefix: Préfixe (dossier logique) sous lequel déposer les fichiers
            dans le bucket.

    Returns:
        La liste des URIs GCS des objets créés (``gs://bucket/prefix/fichier``).

    Raises:
        EnvironmentError: Si la variable ``GCS_BUCKET`` n'est pas définie.
        FileNotFoundError: Si le répertoire local n'existe pas.
    """
    # 1. Charger le fichier .env pour récupérer la configuration.
    load_dotenv()

    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        raise EnvironmentError(
            "Variable GCS_BUCKET non définie. Renseignez-la dans le fichier .env."
        )

    # GCP_PROJECT est facultatif : l'ADC connaît déjà le projet par défaut.
    project = os.getenv("GCP_PROJECT")

    source_dir = Path(local_dir)
    if not source_dir.exists():
        raise FileNotFoundError(
            f"Répertoire introuvable : {source_dir}. Lancez d'abord l'extraction."
        )

    prefix = gcs_prefix.strip("/")

    # 2. Instancier le client GCS (authentification implicite via ADC).
    client = storage.Client(project=project) if project else storage.Client()
    bucket = client.bucket(bucket_name)

    fichiers = sorted(source_dir.glob("*.parquet"))
    if not fichiers:
        logger.warning("Aucun fichier .parquet trouvé dans %s", source_dir)
        return []

    uris: list[str] = []

    # 3. Téléverser chaque fichier.
    for fichier in fichiers:
        chemin_distant = f"{prefix}/{fichier.name}"
        blob = bucket.blob(chemin_distant)
        uri = f"gs://{bucket_name}/{chemin_distant}"
        taille_locale = fichier.stat().st_size

        # Idempotence : on ignore un objet déjà présent et de même taille.
        if blob.exists():
            blob.reload()  # récupère les métadonnées, dont la taille 
            if blob.size == taille_locale: 
                logger.info("Déjà présent, ignoré : %s", uri)
                uris.append(uri)
                continue
            logger.warning("Taille différente, re-téléversement : %s", uri)

        logger.info(
            "Téléversement de %s (%.1f Mo) vers %s",
            fichier.name, taille_locale / (1024 * 1024), uri,
        )
        # timeout élargi : les fichiers TLC peuvent peser plusieurs centaines de Mo. 
        blob.upload_from_filename(str(fichier), timeout=600)

        logger.info("Téléversé : %s", uri)
        uris.append(uri) 

    logger.info("Chargement GCS terminé : %d objet(s) dans le bucket.", len(uris))
    return uris 


if __name__ == "__main__":
    upload_to_gcs() 