-- =============================================================================
-- Requêtes analytiques — uber-tlc-pipeline
-- =============================================================================
-- Requêtes de référence répondant aux questions métier du projet et servant de
-- base aux dashboards Looker.
--
-- Remplacer {{PROJECT}} et {{DATASET}} avant exécution.
-- La table fact_trips impose un filtre de partition : toujours contraindre
-- DATE(pickup_datetime).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Volume de courses par jour
-- Suit la tendance globale de la demande et détecte les trous d'ingestion.
-- -----------------------------------------------------------------------------
SELECT
  DATE(pickup_datetime)  AS jour,
  COUNT(*)               AS nb_courses,
  ROUND(AVG(trip_miles), 2)      AS distance_moyenne_miles,
  ROUND(AVG(total_amount), 2)    AS montant_moyen_usd
FROM `{{PROJECT}}.{{DATASET}}.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY jour
ORDER BY jour;


-- -----------------------------------------------------------------------------
-- 2. Demande par heure et par jour de la semaine
-- Identifie les créneaux de pointe (heatmap Looker).
-- -----------------------------------------------------------------------------
SELECT
  pickup_dayofweek       AS jour_semaine,
  pickup_hour            AS heure,
  COUNT(*)               AS nb_courses,
  ROUND(AVG(trip_duration_min), 1) AS duree_moyenne_min
FROM `{{PROJECT}}.{{DATASET}}.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY jour_semaine, heure
ORDER BY jour_semaine, heure;


-- -----------------------------------------------------------------------------
-- 3. Top 20 des zones de prise en charge
-- Croise la table de faits avec le référentiel géographique.
-- -----------------------------------------------------------------------------
SELECT
  z.borough                     AS arrondissement,
  z.zone_name                   AS zone,
  COUNT(*)                      AS nb_courses,
  ROUND(SUM(f.total_amount), 2) AS revenu_total_usd
FROM `{{PROJECT}}.{{DATASET}}.fact_trips` AS f
JOIN `{{PROJECT}}.{{DATASET}}.dim_zones`  AS z
  ON f.pickup_location_id = z.location_id
WHERE DATE(f.pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY arrondissement, zone
ORDER BY nb_courses DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 4. Axes origine → destination les plus fréquentés
-- -----------------------------------------------------------------------------
SELECT
  o.zone_name                   AS zone_depart,
  d.zone_name                   AS zone_arrivee,
  COUNT(*)                      AS nb_courses,
  ROUND(AVG(f.trip_miles), 2)   AS distance_moyenne_miles,
  ROUND(AVG(f.total_amount), 2) AS montant_moyen_usd
FROM `{{PROJECT}}.{{DATASET}}.fact_trips` AS f
JOIN `{{PROJECT}}.{{DATASET}}.dim_zones`  AS o ON f.pickup_location_id  = o.location_id
JOIN `{{PROJECT}}.{{DATASET}}.dim_zones`  AS d ON f.dropoff_location_id = d.location_id
WHERE DATE(f.pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY zone_depart, zone_arrivee
ORDER BY nb_courses DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 5. Comportement de pourboire par type de service
-- -----------------------------------------------------------------------------
SELECT
  service_type                                          AS type_service,
  COUNT(*)                                              AS nb_courses,
  ROUND(AVG(tips), 2)                                   AS pourboire_moyen_usd,
  ROUND(AVG(SAFE_DIVIDE(tips, NULLIF(base_fare, 0))) * 100, 1) AS taux_pourboire_pct,
  ROUND(COUNTIF(tips > 0) / COUNT(*) * 100, 1)          AS part_courses_avec_pourboire_pct
FROM `{{PROJECT}}.{{DATASET}}.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY type_service
ORDER BY nb_courses DESC;


-- -----------------------------------------------------------------------------
-- 6. Contrôle qualité : détection des valeurs aberrantes résiduelles
-- À exécuter après chaque chargement pour valider l'étape de transformation.
-- -----------------------------------------------------------------------------
SELECT
  COUNT(*)                                        AS total_lignes,
  COUNTIF(trip_miles <= 0)                        AS distance_invalide,
  COUNTIF(trip_duration_min <= 0)                 AS duree_invalide,
  COUNTIF(avg_speed_mph > 100)                    AS vitesse_aberrante,
  COUNTIF(total_amount < 0)                       AS montant_negatif,
  COUNTIF(pickup_location_id IS NULL)             AS zone_manquante
FROM `{{PROJECT}}.{{DATASET}}.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31';


-- TODO : ajouter les requêtes d'analyse de saisonnalité et de comparaison
--        année sur année une fois plusieurs années chargées.
