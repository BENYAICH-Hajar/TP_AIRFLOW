from datetime import datetime, timedelta
import os
import subprocess
import logging
import tempfile
from urllib.parse import quote

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

SEUIL_ERREUR_PCT = 5.0
WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"

default_args = {
    "owner": "hajar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def webhdfs_mkdirs(hdfs_path: str) -> None:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.put(
        url,
        params={"op": "MKDIRS", "user.name": HDFS_USER},
        timeout=30,
    )
    resp.raise_for_status()


def webhdfs_test_exists(hdfs_path: str) -> bool:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.get(
        url,
        params={"op": "GETFILESTATUS", "user.name": HDFS_USER},
        timeout=30,
    )
    return resp.status_code == 200


def webhdfs_create_file(local_path: str, hdfs_path: str) -> None:
    """
    Upload d'un fichier local vers HDFS via WebHDFS.
    Processus WebHDFS :
    1) PUT op=CREATE sur NameNode -> reçoit une redirection 307
    2) PUT du contenu vers l'URL de redirection
    """
    create_url = f"{WEBHDFS_BASE}{hdfs_path}"

    # Étape 1 : demande de création
    resp = requests.put(
        create_url,
        params={
            "op": "CREATE",
            "overwrite": "true",
            "user.name": HDFS_USER,
        },
        allow_redirects=False,
        timeout=30,
    )

    if resp.status_code not in (307, 201):
        raise RuntimeError(
            f"Échec CREATE WebHDFS pour {hdfs_path}: "
            f"status={resp.status_code}, body={resp.text}"
        )

    redirect_url = resp.headers.get("Location")
    if not redirect_url:
        raise RuntimeError(f"Pas de redirection WebHDFS reçue pour {hdfs_path}")

    # Étape 2 : upload réel
    with open(local_path, "rb") as f:
        upload_resp = requests.put(
            redirect_url,
            data=f,
            headers={"Content-Type": "application/octet-stream"},
            timeout=120,
        )
    upload_resp.raise_for_status()


def webhdfs_open_text(hdfs_path: str) -> str:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.get(
        url,
        params={"op": "OPEN", "user.name": HDFS_USER},
        allow_redirects=True,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def webhdfs_rename(src_path: str, dst_path: str) -> None:
    url = f"{WEBHDFS_BASE}{src_path}"
    resp = requests.put(
        url,
        params={
            "op": "RENAME",
            "destination": dst_path,
            "user.name": HDFS_USER,
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("boolean", False):
        raise RuntimeError(f"Le renommage HDFS a échoué: {src_path} -> {dst_path}")


def generer_logs_journaliers(**context):
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    logging.info(f"[INFO] Génération des logs pour la date {execution_date}")

    subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True,
    )

    if not os.path.exists(fichier_sortie):
        raise FileNotFoundError(f"Le fichier {fichier_sortie} n'a pas été généré")

    taille = os.path.getsize(fichier_sortie)
    logging.info(f"[OK] Fichier généré : {fichier_sortie} ({taille} octets)")

    return fichier_sortie


def uploader_vers_hdfs(**context):
    execution_date = context["ds"]
    fichier_local = f"/tmp/access_{execution_date}.log"
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    if not os.path.exists(fichier_local):
        raise FileNotFoundError(f"Fichier local introuvable: {fichier_local}")

    # Crée les dossiers HDFS si besoin
    webhdfs_mkdirs("/data")
    webhdfs_mkdirs("/data/ecommerce")
    webhdfs_mkdirs("/data/ecommerce/logs")
    webhdfs_mkdirs("/data/ecommerce/logs/raw")
    webhdfs_mkdirs("/data/ecommerce/logs/processed")

    logging.info(f"[INFO] Upload WebHDFS : {fichier_local} -> {chemin_hdfs}")
    webhdfs_create_file(fichier_local, chemin_hdfs)

    if not webhdfs_test_exists(chemin_hdfs):
        raise FileNotFoundError(f"Fichier absent dans HDFS après upload: {chemin_hdfs}")

    logging.info(f"[OK] Fichier présent dans HDFS : {chemin_hdfs}")
    return chemin_hdfs


def hdfs_file_sensor(**context):
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    logging.info(f"[INFO] Vérification du fichier HDFS : {chemin_hdfs}")

    if not webhdfs_test_exists(chemin_hdfs):
        raise FileNotFoundError(f"Fichier HDFS absent : {chemin_hdfs}")

    logging.info("[OK] Fichier présent dans HDFS")


def analyser_logs_hdfs(**context):
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    fichier_local = f"/tmp/logs_analyse_{execution_date}.txt"
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

    logging.info(f"[INFO] Lecture du fichier HDFS : {chemin_hdfs}")
    contenu = webhdfs_open_text(chemin_hdfs)

    with open(fichier_local, "w", encoding="utf-8") as f:
        f.write(contenu)

    lignes = [line for line in contenu.splitlines() if line.strip()]
    total = len(lignes)
    erreurs = 0
    status_counts = {}
    url_counts = {}

    for line in lignes:
        try:
            # Exemple :
            # IP - - [date] "GET /url HTTP/1.1" 200 1234 "ref" "ua"
            first_quote = line.find('"')
            second_quote = line.find('"', first_quote + 1)
            request_part = line[first_quote + 1:second_quote]  # GET /url HTTP/1.1

            after_request = line[second_quote + 1:].strip()
            status_str = after_request.split()[0]
            status = int(status_str)

            parts = request_part.split()
            method = parts[0]
            url = parts[1]

            status_counts[status] = status_counts.get(status, 0) + 1
            url_counts[url] = url_counts.get(url, 0) + 1

            if 400 <= status <= 599:
                erreurs += 1
        except Exception:
            logging.warning(f"[WARN] Ligne ignorée car non parsable : {line}")

    logging.info("=== STATUS CODES ===")
    for code, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        logging.info(f"{code}: {count}")

    logging.info("=== TOP 5 URLS ===")
    top_urls = sorted(url_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    for url, count in top_urls:
        logging.info(f"{url}: {count}")

    logging.info("=== TAUX ERREUR ===")
    logging.info(f"Total: {total}, Erreurs: {erreurs}")

    with open(fichier_taux, "w", encoding="utf-8") as f:
        f.write(f"{erreurs} {total}")

    logging.info(f"[INFO] Fichier taux écrit : {fichier_taux}")


def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

    if not os.path.exists(fichier_taux):
        raise FileNotFoundError(f"Fichier taux introuvable : {fichier_taux}")

    with open(fichier_taux, "r", encoding="utf-8") as f:
        contenu = f.read().strip()

    erreurs_str, total_str = contenu.split()
    erreurs = int(erreurs_str)
    total = int(total_str)

    taux_pct = 0.0 if total == 0 else (erreurs / total) * 100

    logging.info(
        f"[INFO] Taux d'erreur calculé : {taux_pct:.2f}% "
        f"(erreurs={erreurs}, total={total})"
    )

    if taux_pct > SEUIL_ERREUR_PCT:
        return "alerter_equipe_ops"
    return "archiver_rapport_ok"


def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    logging.warning(
        f"[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du {execution_date}. "
        "Vérifiez les serveurs web."
    )


def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    logging.info(
        f"[OK] Taux d'erreur dans les seuils normaux pour les logs du {execution_date}."
    )


def archiver_logs_hdfs(**context):
    execution_date = context["ds"]
    src = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    dst = f"/data/ecommerce/logs/processed/access_{execution_date}.log"

    if not webhdfs_test_exists(src):
        raise FileNotFoundError(f"Fichier source absent avant archivage : {src}")

    logging.info(f"[INFO] Déplacement HDFS : {src} -> {dst}")
    webhdfs_rename(src, dst)

    if not webhdfs_test_exists(dst):
        raise FileNotFoundError(f"Fichier destination absent après archivage : {dst}")

    logging.info(f"[OK] Fichier archivé dans : {dst}")


with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline ETL logs e-commerce vers HDFS via WebHDFS",
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["hdfs", "logs", "ecommerce", "airflow"],
) as dag:

    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    t_upload = PythonOperator(
        task_id="uploader_vers_hdfs",
        python_callable=uploader_vers_hdfs,
    )

    t_sensor = PythonOperator(
        task_id="hdfs_file_sensor",
        python_callable=hdfs_file_sensor,
    )

    t_analyser = PythonOperator(
        task_id="analyser_logs_hdfs",
        python_callable=analyser_logs_hdfs,
    )

    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    t_archiver = PythonOperator(
        task_id="archiver_logs_hdfs",
        python_callable=archiver_logs_hdfs,
        trigger_rule="none_failed_min_one_success",
    )

    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )