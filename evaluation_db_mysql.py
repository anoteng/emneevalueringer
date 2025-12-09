"""
MariaDB-backed evaluation_db replacement for read-only operations.

Denne modulen gir samme offentlige API som den originale SQLite-baserte
evaluation_db for lesing:

    get_subject_overview_df(db_name, subject_code, include_stats=True, columns=None)
    get_subjects_df(db_name)
    get_subjects(db_name, search=None, limit=None)

Den bruker tabellene du nettopp har migrert til MariaDB-databasen `aol`:

    course_eval_subject  (id, name)
    course_eval_question (id, label, display_order)
    course_eval          (id, subject_id, year, term)
    course_eval_result   (id, evaluation_id, question_id, value)
    course_eval_stats    (evaluation_id, answered, invited, response_percent)

Tilkobling styres av miljøvariabler:

    EVAL_DB_HOST      (default: "localhost")
    EVAL_DB_PORT      (default: "3306")
    EVAL_DB_USER      (default: "aol")
    EVAL_DB_PASSWORD  (default: "")
    EVAL_DB_NAME      (default: db_name-argumentet, ellers "aol")

`db_name`-argumentet som sendes inn fra eksisterende kode blir altså tolket
som "logisk databasenavn", ikke som SQLite-fil.
"""

from __future__ import annotations

import os
from typing import Iterable, Optional

import pandas as pd
import pymysql


def _get_connection(db_name: str | None = None) -> pymysql.connections.Connection:
    """Opprett en MariaDB-tilkobling basert på miljøvariabler."""
    host = os.environ.get("EVAL_DB_HOST", "localhost")
    port_str = os.environ.get("EVAL_DB_PORT", "3306")
    try:
        port = int(port_str)
    except ValueError:
        port = 3306

    user = os.environ.get("EVAL_DB_USER", "aol")
    password = os.environ.get("EVAL_DB_PASSWORD", "")
    name = os.environ.get("EVAL_DB_NAME") or db_name or "aol"

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_subject_overview_df(
    db_path: str,
    subject_code: str,
    include_stats: bool = True,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Returner en DataFrame med evalueringer for gitt emnekode.

    Kolonner:
      - "År"
      - én kolonne per spørsmål: "<id> <label>", f.eks. "1.1 Faglig innhold"
      - hvis include_stats=True: "Antall svar", "Antall invitert", "Svar%"
    """
    conn = _get_connection(db_path)
    try:
        # Alle enkeltresultater for emnet
        sql = """
            SELECT
                e.year AS year,
                q.id   AS question_id,
                q.label AS question_label,
                r.value AS value
            FROM course_eval_result AS r
            JOIN course_eval AS e
              ON e.id = r.evaluation_id
            JOIN course_eval_question AS q
              ON q.id = r.question_id
            WHERE e.subject_id = %s
        """
        df = pd.read_sql(sql, conn, params=[subject_code])
        if df.empty:
            return pd.DataFrame()

        # Kolonnenavn i stil med "1.1 Spørsmålslabel"
        df["question"] = df["question_id"].astype(str) + " " + df["question_label"].astype(str)

        # Filtrer på ønskede spørsmål (prefiks på question_id) hvis oppgitt
        if columns is not None:
            prefixes = set(columns)
            df = df[df["question_id"].apply(lambda qid: any(qid.startswith(p) for p in prefixes))]

        # Pivot: én rad per år, én kolonne per spørsmål
        table = df.pivot_table(
            index="year",
            columns="question",
            values="value",
            aggfunc="first",
        )
        table = table.reset_index().rename(columns={"year": "År"})

        if include_stats:
            stats_sql = """
                SELECT
                    e.year AS year,
                    SUM(s.answered) AS answered,
                    SUM(s.invited) AS invited
                FROM course_eval_stats AS s
                JOIN course_eval AS e
                  ON e.id = s.evaluation_id
                WHERE e.subject_id = %s
                GROUP BY e.year
            """
            stats = pd.read_sql(stats_sql, conn, params=[subject_code])
            if not stats.empty:
                # Beregn svarprosent
                def _calc_pct(row):
                    invited = row["invited"]
                    answered = row["answered"]
                    if invited in (0, None):
                        return None
                    try:
                        return float(answered) / invited * 100
                    except Exception:
                        return None

                stats["Svar%"] = stats.apply(_calc_pct, axis=1)
                stats = stats.rename(
                    columns={
                        "year": "År",
                        "answered": "Antall svar",
                        "invited": "Antall invitert",
                    }
                )
                table = table.merge(stats, on="År", how="left")

        # Ryddig kolonnerekkefølge: År først, så spørsmål, stats til slutt
        cols = list(table.columns)
        base_cols = ["År"]
        stat_cols = [c for c in ["Antall svar", "Antall invitert", "Svar%"] if c in cols]
        question_cols = [c for c in cols if c not in base_cols + stat_cols]
        ordered = base_cols + sorted(question_cols) + stat_cols
        table = table[ordered]

        return table
    finally:
        conn.close()


def get_subjects_df(db_path: str) -> pd.DataFrame:
    """Returner en DataFrame med alle emner som har evalueringer."""
    conn = _get_connection(db_path)
    try:
        sql = """
            SELECT id, name
            FROM course_eval_subject
            ORDER BY id
        """
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def get_subjects(
    db_path: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Returner liste over emner med antall evalueringer og årsspenn.

    Hvert element er:
        {
          "id": ...,
          "name": ...,
          "evaluations": <antall evalueringer>,
          "year_min": <første år>,
          "year_max": <siste år>,
        }
    """
    conn = _get_connection(db_path)
    try:
        base_sql = """
            SELECT
                s.id AS id,
                COALESCE(s.name, '') AS name,
                COUNT(DISTINCT e.id) AS evaluations,
                MIN(e.year) AS year_min,
                MAX(e.year) AS year_max
            FROM course_eval_subject AS s
            LEFT JOIN course_eval AS e
              ON e.subject_id = s.id
        """
        where = []
        params: list = []
        if search:
            where.append("(s.id LIKE %s OR s.name LIKE %s)")
            like = f"%{search}%"
            params.extend([like, like])
        if where:
            base_sql += " WHERE " + " AND ".join(where)
        base_sql += " GROUP BY s.id, s.name ORDER BY s.id"
        if limit is not None and limit > 0:
            base_sql += f" LIMIT {int(limit)}"

        with conn.cursor() as cur:
            cur.execute(base_sql, params)
            rows = cur.fetchall()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "id": r["id"],
                    "name": r["name"],
                    "evaluations": int(r["evaluations"]) if r["evaluations"] is not None else 0,
                    "year_min": int(r["year_min"]) if r["year_min"] is not None else None,
                    "year_max": int(r["year_max"]) if r["year_max"] is not None else None,
                }
            )
        return result
    finally:
        conn.close()