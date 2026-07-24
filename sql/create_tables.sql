-- =============================================================================
-- DDL du data warehouse BigQuery — uber-tlc-pipeline
-- =============================================================================
-- Remplacer les placeholders avant exécution :
--   {{PROJECT}} -> identifiant du projet GCP (variable GCP_PROJECT)
--   {{DATASET}} -> dataset BigQuery cible  (variable BQ_DATASET)
--
-- Exécution : bq query --use_legacy_sql=false < sql/create_tables.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Dataset analytique
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `{{PROJECT}}.{{DATASET}}`
OPTIONS (
  location = 'US',
  description = "Données de trajets NYC TLC nettoyées et enrichies"
);


-- -----------------------------------------------------------------------------
-- Table de faits : une ligne par course
-- Partitionnée par jour de prise en charge et clusterisée par zones, afin de
-- réduire le volume scanné par les requêtes filtrées sur date ou zone.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{{PROJECT}}.{{DATASET}}.fact_trips`
(
  trip_id             STRING     NOT NULL OPTIONS (description = "Identifiant unique de la course"),
  pickup_datetime     TIMESTAMP  NOT NULL OPTIONS (description = "Horodatage de la prise en charge"),
  dropoff_datetime    TIMESTAMP  NOT NULL OPTIONS (description = "Horodatage de la dépose"),
  pickup_location_id  INT64               OPTIONS (description = "Zone TLC de prise en charge"),
  dropoff_location_id INT64               OPTIONS (description = "Zone TLC de dépose"),
  trip_miles          FLOAT64             OPTIONS (description = "Distance parcourue, en miles"),
  trip_duration_min   FLOAT64             OPTIONS (description = "Durée du trajet, en minutes"),
  avg_speed_mph       FLOAT64             OPTIONS (description = "Vitesse moyenne, en miles/heure"),
  base_fare           FLOAT64             OPTIONS (description = "Montant de base de la course, en USD"),
  tips                FLOAT64             OPTIONS (description = "Pourboire, en USD"),
  tolls               FLOAT64             OPTIONS (description = "Péages, en USD"),
  total_amount        FLOAT64             OPTIONS (description = "Montant total payé, en USD"),
  service_type        STRING              OPTIONS (description = "Type de service : yellow, green, fhv, fhvhv"),
  pickup_hour         INT64               OPTIONS (description = "Heure de prise en charge (0-23)"),
  pickup_dayofweek    INT64               OPTIONS (description = "Jour de la semaine (1 = dimanche)")
)
PARTITION BY DATE(pickup_datetime)
CLUSTER BY pickup_location_id, dropoff_location_id
OPTIONS (
  description = "Table de faits des courses NYC TLC",
  require_partition_filter = TRUE  -- force un filtre de date : protège des scans complets
);


-- -----------------------------------------------------------------------------
-- Dimension : zones TLC (référentiel géographique)
-- Table de petite taille, ni partitionnée ni clusterisée.
-- Source : taxi_zone_lookup.csv publié par la TLC.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{{PROJECT}}.{{DATASET}}.dim_zones`
(
  location_id   INT64  NOT NULL OPTIONS (description = "Identifiant de la zone TLC"),
  borough       STRING          OPTIONS (description = "Arrondissement de New York"),
  zone_name     STRING          OPTIONS (description = "Nom de la zone"),
  service_zone  STRING          OPTIONS (description = "Zone de service TLC")
)
OPTIONS (
  description = "Référentiel des zones géographiques TLC"
);


-- TODO : ajouter les vues d'agrégation servant de sources aux dashboards Looker
--        (ex. vue journalière par zone, vue horaire par type de service).
