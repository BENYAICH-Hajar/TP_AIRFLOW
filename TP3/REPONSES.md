# REPONSES.md

# TP DVF — Airflow, HDFS et PostgreSQL

## 1. Objectif du TP

L’objectif de ce TP est de mettre en place un pipeline ETL orchestré avec **Apache Airflow** afin de traiter les données **DVF (Demandes de Valeurs Foncières)**.

Le pipeline doit permettre de :

- vérifier la disponibilité des sources
- télécharger le fichier DVF
- stocker le fichier brut dans **HDFS**
- lire et traiter les données
- calculer des agrégations immobilières sur Paris
- insérer les résultats dans **PostgreSQL**
- générer un rapport final

---

## 2. Architecture mise en place

L’architecture du TP repose sur plusieurs services Docker :

- **Airflow Webserver** : interface web Airflow
- **Airflow Scheduler** : orchestration des tâches
- **PostgreSQL** : base de données relationnelle
- **HDFS NameNode** : gestion du système de fichiers distribué
- **HDFS DataNode** : stockage des blocs HDFS

Le tout est lancé avec **Docker Compose**.

---

## 3. Fichiers du projet

Les fichiers principaux utilisés sont :

- `docker-compose.yaml`
- `sql/init_dvf.sql`
- `dags/dag_dvf.py`

Le dossier contient aussi :

- `dags/`
- `logs/`
- `plugins/`
- `sql/`

---

## 4. Base de données PostgreSQL

La base PostgreSQL contient les tables suivantes :

### `dvf_raw`
Table destinée aux données brutes DVF.

### `prix_m2_arrondissement`
Table qui stocke les agrégats calculés par arrondissement :

- code postal
- arrondissement
- année
- mois
- prix moyen au m²
- prix médian au m²
- prix min au m²
- prix max au m²
- nombre de transactions
- surface moyenne

### `stats_marche`
Table contenant les statistiques globales du marché parisien :

- année
- mois
- nombre total de transactions
- prix médian Paris
- prix moyen Paris
- arrondissement le plus cher
- arrondissement le moins cher
- surface médiane

---

## 5. Description du DAG Airflow

Le DAG s’appelle :

```python
pipeline_dvf_immobilier