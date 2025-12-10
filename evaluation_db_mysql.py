import os
from typing import Iterable, Optional

import pymysql
import pandas as pd


def _get_connection(db_name: str):
    """Åpne en MariaDB-connection basert på miljøvariabler."""
    host = os.environ.get("EVAL_DB_HOST", "localhost")
    port = int(os.environ.get("EVAL_DB_PORT", "3306"))
    user = os.environ.get("EVAL_DB_USER", "aol")
    password = os.environ.get("EVAL_DB_PASSWORD", "")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


# ---------------------------------------------------------------------------
# Emneliste – gjenbruker courses
# ---------------------------------------------------------------------------

def get_subjects(
    db_name: str,
    search: Optional[str] = None,
    limit: int = 2000,
) -> list[dict]:
    """
    Hent liste over emner som har evalueringer.

    Bruker `courses` + `course_eval` i AOL-databasen.
    """
    conn = _get_connection(db_name)
    try:
        sql = """
            SELECT
              c.course_code AS id,
              COALESCE(c.name_no, c.name_eng, c.course_code) AS name,
              COUNT(DISTINCT e.id) AS evaluations,
              MIN(e.year) AS year_min,
              MAX(e.year) AS year_max
            FROM course_eval e
            JOIN courses c ON e.course_id = c.id
        """
        params: list = []

        if search:
            sql += """
                WHERE
                  c.course_code LIKE %s
                  OR c.name_no LIKE %s
                  OR c.name_eng LIKE %s
            """
            like = f"%{search}%"
            params.extend([like, like, like])

        sql += """
            GROUP BY c.course_code, name
            ORDER BY c.course_code
            LIMIT %s
        """
        params.append(limit)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        # matcha gammel API-struktur: id, name, evaluations, year_min, year_max
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "evaluations": int(r["evaluations"] or 0),
                "year_min": int(r["year_min"]) if r["year_min"] is not None else None,
                "year_max": int(r["year_max"]) if r["year_max"] is not None else None,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_subjects_df(
    db_name: str,
    search: Optional[str] = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """Samme som get_subjects, men som DataFrame."""
    rows = get_subjects(db_name, search=search, limit=limit)
    if not rows:
        return pd.DataFrame(columns=["id", "name", "evaluations", "year_min", "year_max"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Oversikt for ett emne – År + Termin + spørsmål + statistikk
# ---------------------------------------------------------------------------

def _get_course_id(conn, course_code: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM courses WHERE course_code = %s",
            (course_code,),
        )
        row = cur.fetchone()
    return row["id"] if row else None


def get_subject_overview_df(
    db_name: str,
    subject_code: str,
    include_stats: bool = True,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Hent oversikt for ett emne (course_code) fra MariaDB/AOL.

    Resultatet er en DataFrame der hver rad er (År, Termin) for én gjennomføring,
    og kolonnene er spørsmål (f.eks. "1.1 Læringsutbytte") + ev. statistikkfelt.

    `columns` er en liste med spørsmålskoder (f.eks. ["1.1", "1.2"]); matches på prefix.
    """
    conn = _get_connection(db_name)
    try:
        course_id = _get_course_id(conn, subject_code)
        if course_id is None:
            return pd.DataFrame()

        # 1) Hent alle svar for emnet
        sql = """
            SELECT
                e.year              AS year,
                e.term              AS term,
                r.question_id       AS question_id,
                COALESCE(q.label, r.question_id) AS question_label,
                r.value             AS value
            FROM course_eval_result r
            JOIN course_eval e
              ON e.id = r.evaluation_id
            LEFT JOIN course_eval_question q
              ON q.id = r.question_id
            WHERE e.course_id = %s
        """
        df = pd.read_sql(sql, conn, params=[course_id])

        if df.empty:
            return pd.DataFrame()

        # 2) Lag "spørsmålsnavn": "1.1 Læringsutbytte"
        df["question"] = df["question_id"].astype(str) + " " + df["question_label"].astype(str)

        # 3) Filtrer på spørsmål dersom columns er gitt
        if columns:
            wanted = list(columns)

            def _keep(qid: str) -> bool:
                return any(str(qid).startswith(prefix) for prefix in wanted)

            df = df[df["question_id"].apply(_keep)]

        if df.empty:
            return pd.DataFrame()

        # 4) Pivot: rader = (year, term), kolonner = spørsmål
        table = df.pivot_table(
            index=["year", "term"],
            columns="question",
            values="value",
            aggfunc="mean",
        )

        table.reset_index(inplace=True)
        table.rename(columns={"year": "År", "term": "Termin"}, inplace=True)

        # 5) Hent statistikk og heng på (per år+termin)
        if include_stats:
            stats_sql = """
                SELECT
                    e.year              AS year,
                    e.term              AS term,
                    s.answered          AS answered,
                    s.invited           AS invited,
                    s.response_percent  AS response_percent
                FROM course_eval_stats s
                JOIN course_eval e
                  ON e.id = s.evaluation_id
                WHERE e.course_id = %s
            """
            stats_df = pd.read_sql(stats_sql, conn, params=[course_id])

            if not stats_df.empty:
                stats_grouped = (
                    stats_df.groupby(["year", "term"], as_index=False)
                    .agg(
                        answered=("answered", "sum"),
                        invited=("invited", "sum"),
                        response_percent=("response_percent", "max"),
                    )
                )
                table = table.merge(
                    stats_grouped,
                    left_on=["År", "Termin"],
                    right_on=["year", "term"],
                    how="left",
                )
                table.drop(columns=["year", "term"], inplace=True, errors="ignore")
                table.rename(
                    columns={
                        "answered": "Antall svar",
                        "invited": "Antall invitert",
                        "response_percent": "Svar%",
                    },
                    inplace=True,
                )

        # 6) Rydd kolonnerekkefølge: År, Termin først
        cols = list(table.columns)
        # trekk ut År og Termin
        other_cols = [c for c in cols if c not in ("År", "Termin")]
        ordered = ["År", "Termin"] + other_cols
        table = table[ordered]

        # Sorter på År, så Termin (alfabetisk – funker fint med 'Vår'/'Høst' etc.)
        table.sort_values(by=["År", "Termin"], inplace=True)

        return table

    finally:
        conn.close()