"""
evaluation_db_mysql.py
~~~~~~~~~~~~~~~~~~~~~~

MariaDB-backend for emneevalueringer, koblet mot AOL-databasen.

Forventer følgende tabeller (som du nettopp har satt opp):

- courses (id, course_code, name_no, name_eng, ...)
- semester (id, name, rank, ...)
- course_eval (id, course_id, year, semester_id, run, ...)
- course_eval_question (id, code, label, display_order)
- course_eval_result (id, evaluation_id, question_id -> course_eval_question.id, value)
- course_eval_stats (evaluation_id -> course_eval.id, answered, invited, response_percent)

Alle funksjonene speiler signaturene fra evaluation_db.py (SQLite-versjonen),
slik at evaluation_api.py kan bruke dem uten endringer.
"""

from __future__ import annotations

import os
from typing import Iterable, Optional

import pandas as pd
import pymysql
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# DB-tilkobling
# ---------------------------------------------------------------------------


def _get_connection(db_name: str):
    """
    Åpne en MariaDB-connection basert på miljøvariabler.

    Bruker:
      - EVAL_DB_HOST (default "localhost")
      - EVAL_DB_PORT (default "3306")
      - EVAL_DB_USER (default "aol")
      - EVAL_DB_PASSWORD (default "")
    """
    host = os.environ.get("EVAL_DB_HOST", "localhost")
    port = int(os.environ.get("EVAL_DB_PORT", "3306"))
    user = os.environ.get("EVAL_DB_USER", "aol")
    password = os.environ.get("EVAL_DB_PASSWORD", "")


    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
        pool_recycle=3600,
        pool_pre_ping=True,
    )

    return engine

    # return pymysql.connect(
    #     host=host,
    #     port=port,
    #     user=user,
    #     password=password,
    #     database=db_name,
    #     charset="utf8mb4",
    #     cursorclass=pymysql.cursors.DictCursor,
    # )


# ---------------------------------------------------------------------------
# Hjelpefunksjoner
# ---------------------------------------------------------------------------


def _get_course_id(conn, course_code: str) -> Optional[int]:
    """Slå opp courses.id basert på course_code."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM courses WHERE course_code = %s",
            (course_code,),
        )
        row = cur.fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# Emneliste – brukes av /api/subject og /api/subjects
# ---------------------------------------------------------------------------


def get_subjects(
    db_path: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Returner en liste med emner som har evalueringer.

    Hver entry har:
      - id: emnekode (courses.course_code)
      - name: norsk/engelsk navn
      - evaluations: antall evalueringer
      - year_min, year_max: første og siste år med evaluering
    """
    conn = _get_connection(db_path)
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

        sql += " GROUP BY c.course_code, name ORDER BY c.course_code"

        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "id": r["id"],
                    "name": r["name"],
                    "evaluations": int(r["evaluations"] or 0),
                    "year_min": int(r["year_min"]) if r["year_min"] is not None else None,
                    "year_max": int(r["year_max"]) if r["year_max"] is not None else None,
                }
            )
        return result

    finally:
        conn.close()


def get_subjects_df(db_path: str) -> pd.DataFrame:
    """
    Returner emnelista som DataFrame (brukes i noen sammenhenger).
    """
    rows = get_subjects(db_path)
    if not rows:
        return pd.DataFrame(columns=["id", "name", "evaluations", "year_min", "year_max"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Oversikt for ett emne – År + Termin + spørsmål + statistikk
# ---------------------------------------------------------------------------


def get_subject_overview_df(
    db_path: str,
    subject_code: str,
    include_stats: bool = True,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Hent oversikt for ett emne (course_code) fra MariaDB/AOL.

    Resultatet er en DataFrame der hver rad representerer én gjennomføring:
      - År
      - Termin (navn fra semester.name; f.eks. Augustblokk, Høstsemester, ...)
      - run (hvis du har flere kjøringer samme år/semester)
      - én kolonne per spørsmål ("1.1 Læringsutbytte" osv.)
      - ev. statistikk: Antall svar, Antall invitert, Svar%

    `columns` kan brukes til å filtrere på spørsmåls-koder (f.eks. ["1.1", "1.2"]),
    da matches det på prefix av question_code.
    """
    conn = _get_connection(db_path)
    try:
        course_id = _get_course_id(conn, subject_code)
        if course_id is None:
            return pd.DataFrame()

        # 1) Hent alle svar for emnet, med semester-navn og rank
        sql = """
            SELECT
                e.year              AS year,
                s.name              AS term_name,
                s.rank              AS semester_rank,
                e.run               AS run,
                q.code              AS question_code,
                q.label             AS question_label,
                r.value             AS value
            FROM course_eval_result r
            JOIN course_eval e
              ON e.id = r.evaluation_id
            JOIN course_eval_question q
              ON q.id = r.question_id
            JOIN semester s
              ON e.semester_id = s.id
            WHERE e.course_id = %s
        """


        df = pd.read_sql(sql, engine, params=[course_id])

        if df.empty:
            return pd.DataFrame()

        # 2) "1.1 Læringsutbytte"
        df["question"] = df["question_code"].astype(str) + " " + df["question_label"].astype(str)

        # 3) Filtrer på spørsmålskoder om ønskelig
        if columns:
            wanted = list(columns)

            def _keep(code: str) -> bool:
                return any(str(code).startswith(prefix) for prefix in wanted)

            df = df[df["question_code"].apply(_keep)]

        if df.empty:
            return pd.DataFrame()

        # 4) Pivot: rader = (year, term_name, semester_rank, run), kolonner = spørsmål
        table = df.pivot_table(
            index=["year", "term_name", "semester_rank", "run"],
            columns="question",
            values="value",
            aggfunc="mean",
        ).reset_index()

        # Gi pene navn ut
        table.rename(
            columns={
                "year": "År",
                "term_name": "Termin",
            },
            inplace=True,
        )

        # 5) Hent og heng på statistikk dersom ønsket
        if include_stats:
            stats_sql = """
                SELECT
                    e.year              AS year,
                    s.name              AS term_name,
                    s.rank              AS semester_rank,
                    e.run               AS run,
                    st.answered         AS answered,
                    st.invited          AS invited,
                    st.response_percent AS response_percent
                FROM course_eval_stats st
                JOIN course_eval e
                  ON e.id = st.evaluation_id
                JOIN semester s
                  ON e.semester_id = s.id
                WHERE e.course_id = %s
            """
            stats_df = pd.read_sql(stats_sql, conn, params=[course_id])

            if not stats_df.empty:
                stats_grouped = (
                    stats_df.groupby(
                        ["year", "term_name", "semester_rank", "run"],
                        as_index=False,
                    )
                    .agg(
                        answered=("answered", "sum"),
                        invited=("invited", "sum"),
                        response_percent=("response_percent", "max"),
                    )
                )

                table = table.merge(
                    stats_grouped,
                    left_on=["År", "Termin", "semester_rank", "run"],
                    right_on=["year", "term_name", "semester_rank", "run"],
                    how="left",
                )

                # rydd vekk duplikat-navn
                table.drop(columns=["year", "term_name"], inplace=True, errors="ignore")

                table.rename(
                    columns={
                        "answered": "Antall svar",
                        "invited": "Antall invitert",
                        "response_percent": "Svar%",
                    },
                    inplace=True,
                )

        # 6) Sorter på År, semester_rank, run
        sort_cols = [c for c in ["År", "semester_rank", "run"] if c in table.columns]
        if sort_cols:
            table.sort_values(by=sort_cols, inplace=True)

        # 7) Fjern interne kolonner fra output
        if "semester_rank" in table.columns:
            table.drop(columns=["semester_rank"], inplace=True)

        # 8) Sett kolonnerekkefølge – År, Termin, run først
        cols = list(table.columns)
        prefix = [c for c in ["År", "Termin", "run"] if c in cols]
        other = [c for c in cols if c not in prefix]
        table = table[prefix + other]

        return table

    finally:
        conn.close()