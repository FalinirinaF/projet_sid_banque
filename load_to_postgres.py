"""
load_to_postgres.py
--------------------
Charge data/resultats_du_jour.csv dans PostgreSQL (table fraud_predictions).
Crée la table si elle n'existe pas ; remplace les données existantes.

Utilisation :
    python load_to_postgres.py
    python load_to_postgres.py --csv data/autre_fichier.csv --table autre_table

Dépendances :
    pip install pandas sqlalchemy psycopg2-binary
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ──────────────────────────────────────────────────────────────
# Configuration par défaut
# ──────────────────────────────────────────────────────────────
DEFAULT_CSV   = "data/resultats_du_jour.csv"
DEFAULT_TABLE = "fraud_predictions"
DEFAULT_DB = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "fraud_dw",
    "user":     "airflow",
    "password": "",          # Définir via variable d'env PG_PASSWORD si nécessaire
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def build_engine(cfg: dict):
    """Crée le moteur SQLAlchemy pour PostgreSQL."""
    import os
    password = os.getenv("PG_PASSWORD", cfg["password"])
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{password}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )
    return create_engine(url, pool_pre_ping=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise les noms de colonnes : minuscules, espaces → underscores."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute des colonnes utiles pour le Data Warehouse."""
    from datetime import datetime, timezone
    df = df.copy()
    df["loaded_at"] = datetime.now(timezone.utc)

    # Score de fraude : dérive depuis 'class' si absent
    if "score_fraude" not in df.columns and "class" in df.columns:
        df["score_fraude"] = df["class"].astype(float)

    # Colonne risque textuelle
    if "score_fraude" in df.columns:
        bins   = [-0.001, 0.5, 0.8, 1.001]
        labels = ["faible", "moyen", "eleve"]
        df["niveau_risque"] = pd.cut(
            df["score_fraude"], bins=bins, labels=labels
        ).astype(str)

    return df


def load_csv(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        log.error("Fichier introuvable : %s", csv_path.resolve())
        sys.exit(1)
    log.info("Lecture de %s …", csv_path)
    df = pd.read_csv(csv_path)
    log.info("  %d lignes, %d colonnes lues.", len(df), len(df.columns))
    return df


def load_to_postgres(df: pd.DataFrame, engine, table: str) -> int:
    """
    Insère le DataFrame dans PostgreSQL.
    if_exists='replace' : recrée la table et remplace toutes les données.
    Retourne le nombre de lignes insérées.
    """
    log.info("Chargement vers la table '%s' …", table)
    df.to_sql(
        name=table,
        con=engine,
        if_exists="replace",   # Remplace les données existantes
        index=False,
        chunksize=1000,        # Insertion par lots pour les gros CSV
        method="multi",        # INSERT multi-lignes (plus rapide)
    )
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    log.info("  %d lignes chargées avec succès.", count)
    return count


def add_index(engine, table: str):
    """Crée des index utiles pour les requêtes du dashboard."""
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_class ON {table} (class);",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_score ON {table} (score_fraude);",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_loaded ON {table} (loaded_at);",
    ]
    with engine.connect() as conn:
        for sql in indexes:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass
    log.info("  Index créés.")


# ──────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Charge un CSV dans PostgreSQL.")
    parser.add_argument("--csv",      default=DEFAULT_CSV,   help="Chemin du fichier CSV")
    parser.add_argument("--table",    default=DEFAULT_TABLE, help="Nom de la table cible")
    parser.add_argument("--host",     default=DEFAULT_DB["host"])
    parser.add_argument("--port",     default=DEFAULT_DB["port"], type=int)
    parser.add_argument("--dbname",   default=DEFAULT_DB["dbname"])
    parser.add_argument("--user",     default=DEFAULT_DB["user"])
    parser.add_argument("--password", default=DEFAULT_DB["password"])
    args = parser.parse_args()

    cfg = {
        "host": args.host, "port": args.port,
        "dbname": args.dbname, "user": args.user, "password": args.password,
    }

    # 1. Lire le CSV
    df = load_csv(args.csv)

    # 2. Normaliser
    df = normalize_columns(df)
    df = add_metadata(df)
    log.info("Colonnes finales : %s", list(df.columns))

    # 3. Connexion PostgreSQL
    try:
        engine = build_engine(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Connexion PostgreSQL OK → %s/%s", cfg["host"], cfg["dbname"])
    except SQLAlchemyError as e:
        log.error("Impossible de se connecter à PostgreSQL : %s", e)
        sys.exit(1)

    # 4. Charger
    try:
        count = load_to_postgres(df, engine, args.table)
        add_index(engine, args.table)
        log.info("Pipeline terminé — %d lignes dans '%s'.", count, args.table)
    except SQLAlchemyError as e:
        log.error("Erreur lors du chargement : %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
