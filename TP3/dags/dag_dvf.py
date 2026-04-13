from __future__ import annotations

import io
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DVF_URL = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20260405-002306/valeursfoncieres-2024.txt.zip"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Airflow-DVF-Pipeline/1.0)"
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _webhdfs_url(path: str, op: str, **params) -> str:
    clean_path = path if path.startswith("/") else f"/{path}"
    query = {"op": op, "user.name": WEBHDFS_USER}
    query.update(params)
    return f"{WEBHDFS_BASE_URL}{clean_path}?{urlencode(query)}"


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule=None,
    start_date=datetime(2026, 4, 12),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts = {"dvf_api": False, "hdfs": False}

        try:
            resp = requests.get(
                DVF_URL,
                headers=HTTP_HEADERS,
                timeout=60,
                allow_redirects=True,
                verify=False,
            )
            logger.info("API DVF status code : %s", resp.status_code)
            statuts["dvf_api"] = resp.ok
        except Exception as exc:
            logger.error("API DVF inaccessible : %s", exc)

        try:
            url = _webhdfs_url("/", "LISTSTATUS")
            resp = requests.get(url, timeout=30)
            logger.info("HDFS status code : %s", resp.status_code)
            statuts["hdfs"] = resp.status_code == 200
        except Exception as exc:
            logger.error("HDFS inaccessible : %s", exc)

        logger.info("Statuts finaux : %s", statuts)

        if not statuts["hdfs"]:
            raise AirflowException("HDFS est inaccessible.")

        if not statuts["dvf_api"]:
            logger.warning("API DVF indisponible ou lente, mais le pipeline continue.")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        local_path = os.path.join(tempfile.gettempdir(), "valeursfoncieres-2024.txt.zip")

        logger.info("Début telecharger_dvf")
        logger.info("Statuts reçus : %s", statuts)
        logger.info("Téléchargement DVF vers : %s", local_path)

        response = requests.get(
            DVF_URL,
            headers=HTTP_HEADERS,
            stream=True,
            timeout=120,
            allow_redirects=True,
            verify=False,
        )

        logger.info("URL finale : %s", response.url)
        logger.info("Status code téléchargement : %s", response.status_code)
        logger.info("Content-Type : %s", response.headers.get("Content-Type"))

        response.raise_for_status()

        total_bytes = 0
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)

        response.close()

        file_size = os.path.getsize(local_path)
        logger.info("Taille finale téléchargée : %s octets", file_size)

        if file_size < 1000:
            raise AirflowException(f"Fichier DVF trop petit ou vide : {file_size} octets")

        logger.info("Téléchargement terminé avec succès")
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        hdfs_filename = "valeursfoncieres-2024.txt.zip"
        hdfs_file_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"

        mkdirs_url = _webhdfs_url(f"{HDFS_RAW_PATH}/", "MKDIRS")
        mkdirs_resp = requests.put(mkdirs_url, timeout=30)
        mkdirs_resp.raise_for_status()

        mkdirs_data = mkdirs_resp.json()
        if not mkdirs_data.get("boolean", False):
            raise AirflowException(f"Impossible de créer le répertoire HDFS : {HDFS_RAW_PATH}")

        create_url = _webhdfs_url(hdfs_file_path, "CREATE", overwrite="true")
        init_resp = requests.put(create_url, allow_redirects=False, timeout=30)

        if init_resp.status_code not in (307, 201):
            raise AirflowException(
                f"Erreur init upload HDFS : {init_resp.status_code} - {init_resp.text}"
            )

        upload_url = init_resp.headers.get("Location")
        if not upload_url:
            raise AirflowException("Aucune URL de redirection reçue pour l'upload WebHDFS.")

        with open(local_path, "rb") as f:
            upload_resp = requests.put(
                upload_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300,
            )

        if upload_resp.status_code != 201:
            raise AirflowException(
                f"Erreur upload final HDFS : {upload_resp.status_code} - {upload_resp.text}"
            )

        logger.info("Fichier stocké dans HDFS : %s", hdfs_file_path)

        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Fichier temporaire supprimé : %s", local_path)

        return hdfs_file_path

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        open_url = _webhdfs_url(hdfs_path, "OPEN")
        response = requests.get(open_url, allow_redirects=True, timeout=300)
        response.raise_for_status()

        logger.info("Lecture du fichier ZIP depuis HDFS : %s", hdfs_path)

        zip_bytes = io.BytesIO(response.content)

        with zipfile.ZipFile(zip_bytes) as zf:
            noms = zf.namelist()
            logger.info("Fichiers dans le zip : %s", noms)

            txt_name = None
            for name in noms:
                if name.lower().endswith(".txt"):
                    txt_name = name
                    break

            if not txt_name:
                raise AirflowException("Aucun fichier .txt trouvé dans l'archive DVF.")

            with zf.open(txt_name) as txt_file:
                df = pd.read_csv(txt_file, sep="|", low_memory=False)

        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("'", "", regex=False)
        )

        logger.info("Colonnes détectées : %s", list(df.columns))
        logger.info("Nombre de lignes avant filtrage : %s", len(df))

        colonnes_obligatoires = [
            "nature_mutation",
            "valeur_fonciere",
            "code_postal",
            "type_local",
            "surface_reelle_bati",
            "code_departement",
        ]
        colonnes_manquantes = [c for c in colonnes_obligatoires if c not in df.columns]
        if colonnes_manquantes:
            raise AirflowException(
                f"Colonnes obligatoires manquantes dans le fichier DVF : {colonnes_manquantes}"
            )

        df["type_local"] = df["type_local"].astype(str).str.strip().str.lower()
        df["nature_mutation"] = df["nature_mutation"].astype(str).str.strip().str.lower()
        df["code_postal"] = df["code_postal"].astype(str).str.extract(r"(\d+)", expand=False)
        df["code_departement"] = df["code_departement"].astype(str).str.extract(r"(\d+)", expand=False)

        for col in ["valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(" ", "", regex=False)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("Valeurs type_local (sample) : %s", df["type_local"].dropna().unique()[:10])
        logger.info("Valeurs nature_mutation (sample) : %s", df["nature_mutation"].dropna().unique()[:10])
        logger.info("Valeurs code_departement (sample) : %s", df["code_departement"].dropna().unique()[:10])
        logger.info("Valeurs code_postal (sample) : %s", df["code_postal"].dropna().unique()[:20])

        codes_postaux_paris = [f"750{i:02d}" for i in range(1, 21)] + ["75116"]

        df = df[
            (df["code_departement"] == "75")
            & (df["type_local"].str.contains("appartement", na=False))
            & (df["nature_mutation"].str.contains("vente", na=False))
            & (df["surface_reelle_bati"] >= 9)
            & (df["surface_reelle_bati"] <= 500)
            & (df["valeur_fonciere"] > 10000)
        ].copy()

        if "code_postal" in df.columns:
            df = df[df["code_postal"].isin(codes_postaux_paris)].copy()

        df = df[df["surface_reelle_bati"] > 0].copy()
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        def extraire_arrondissement(cp: str):
            if pd.isna(cp):
                return None
            cp = str(cp)
            if cp == "75116":
                return 16
            if cp.startswith("750") and len(cp) == 5:
                return int(cp[-2:])
            return None

        df["arrondissement"] = df["code_postal"].apply(extraire_arrondissement)
        df = df[df["arrondissement"].notna()].copy()
        df["arrondissement"] = df["arrondissement"].astype(int)

        logger.info("Nombre de lignes après filtrage : %s", len(df))

        if df.empty:
            raise AirflowException("Aucune donnée valide après filtrage DVF.")

        now = datetime.now()
        annee = now.year
        mois = now.month

        grouped = (
            df.groupby(["code_postal", "arrondissement"])
            .agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean"),
            )
            .reset_index()
        )

        grouped["annee"] = annee
        grouped["mois"] = mois

        agregats = []
        for _, row in grouped.iterrows():
            agregats.append(
                {
                    "code_postal": str(row["code_postal"]),
                    "arrondissement": int(row["arrondissement"]),
                    "annee": int(row["annee"]),
                    "mois": int(row["mois"]),
                    "prix_m2_moyen": round(float(row["prix_m2_moyen"]), 2),
                    "prix_m2_median": round(float(row["prix_m2_median"]), 2),
                    "prix_m2_min": round(float(row["prix_m2_min"]), 2),
                    "prix_m2_max": round(float(row["prix_m2_max"]), 2),
                    "nb_transactions": int(row["nb_transactions"]),
                    "surface_moyenne": round(float(row["surface_moyenne"]), 2),
                }
            )

        stats_globales = {
            "annee": annee,
            "mois": mois,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris": round(float(df["prix_m2"].median()), 2),
            "prix_m2_moyen_paris": round(float(df["prix_m2"].mean()), 2),
            "arrdt_plus_cher": int(grouped.sort_values("prix_m2_median", ascending=False).iloc[0]["arrondissement"]),
            "arrdt_moins_cher": int(grouped.sort_values("prix_m2_median", ascending=False).iloc[-1]["arrondissement"]),
            "surface_mediane": round(float(df["surface_reelle_bati"].median()), 2),
        }

        return {"agregats": agregats, "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        upsert_arrdt = """
        INSERT INTO prix_m2_arrondissement (
            code_postal, arrondissement, annee, mois,
            prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
            nb_transactions, surface_moyenne, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (code_postal, annee, mois)
        DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen,
            prix_m2_median = EXCLUDED.prix_m2_median,
            prix_m2_min = EXCLUDED.prix_m2_min,
            prix_m2_max = EXCLUDED.prix_m2_max,
            nb_transactions = EXCLUDED.nb_transactions,
            surface_moyenne = EXCLUDED.surface_moyenne,
            updated_at = NOW();
        """

        for agg in agregats:
            hook.run(
                upsert_arrdt,
                parameters=(
                    agg["code_postal"],
                    agg["arrondissement"],
                    agg["annee"],
                    agg["mois"],
                    agg["prix_m2_moyen"],
                    agg["prix_m2_median"],
                    agg["prix_m2_min"],
                    agg["prix_m2_max"],
                    agg["nb_transactions"],
                    agg["surface_moyenne"],
                ),
            )

        upsert_stats = """
        INSERT INTO stats_marche (
            annee, mois, nb_transactions_total,
            prix_m2_median_paris, prix_m2_moyen_paris,
            arrdt_plus_cher, arrdt_moins_cher, surface_mediane, date_calcul
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (annee, mois)
        DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """

        if stats_globales:
            hook.run(
                upsert_stats,
                parameters=(
                    stats_globales["annee"],
                    stats_globales["mois"],
                    stats_globales["nb_transactions_total"],
                    stats_globales["prix_m2_median_paris"],
                    stats_globales["prix_m2_moyen_paris"],
                    stats_globales["arrdt_plus_cher"],
                    stats_globales["arrdt_moins_cher"],
                    stats_globales["surface_mediane"],
                ),
            )

        return len(agregats)

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        now = datetime.now()
        query = """
        SELECT
            arrondissement,
            prix_m2_median,
            prix_m2_moyen,
            nb_transactions,
            surface_moyenne
        FROM prix_m2_arrondissement
        WHERE annee = %s AND mois = %s
        ORDER BY prix_m2_median DESC
        LIMIT 20;
        """

        records = hook.get_records(query, parameters=(now.year, now.month))

        lines = []
        lines.append("Arrondissement | Median (EUR/m2) | Moyen (EUR/m2) | Transactions | Surface moyenne")
        lines.append("---------------|-----------------|----------------|--------------|----------------")

        for row in records:
            arrdt, mediane, moyenne, nb_tx, surface = row
            lines.append(
                f"{arrdt:>13} | "
                f"{round(float(mediane), 0):>15.0f} | "
                f"{round(float(moyenne), 0):>14.0f} | "
                f"{int(nb_tx):>12} | "
                f"{round(float(surface), 1):>14.1f}"
            )

        rapport = "\n".join(lines)
        logger.info("\n%s", rapport)
        return rapport

    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)

    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)


pipeline_dvf()