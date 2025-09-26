"""
evaluation_db.py
~~~~~~~~~~~~~~~~~

This module implements a simple SQLite persistence layer for storing and
querying course evaluation data.  It defines a schema of subjects,
evaluations, questions, responses and summary statistics.  The central
function exposed is :func:`get_subject_overview_df` which returns a
``pandas.DataFrame`` summarising the results for a given subject across
all years.

The schema is minimal and follows the conventions established during the
conversation with the user.  If your database does not match this schema,
you may need to adjust the queries accordingly.
"""

from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

import pandas as pd


def get_subject_overview_df(
    db_path: str,
    subject_code: str,
    include_stats: bool = True,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Return a DataFrame summarising evaluations for the given subject.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database file.
    subject_code: str
        The code of the subject to retrieve (e.g. ``"AOS120"``).
    include_stats: bool, optional
        Whether to include the summary statistics columns (``Antall svar``,
        ``Antall invitert`` and ``Svar%``).  Defaults to ``True``.
    columns: Iterable[str], optional
        An optional list of question identifiers (e.g. ``["1.1", "1.2"]``).
        If provided, only these questions will be included in the result.  The
        identifiers are matched by prefix against the ``Question.id`` column.

    Returns
    -------
    pandas.DataFrame
        A DataFrame where each row corresponds to a year and each column
        corresponds to a question (with its label) or a statistics field.
    """
    conn = sqlite3.connect(db_path)
    try:
        # Determine how evaluations reference subjects.  Newer schemas use
        # ``subject_code`` on the Evaluation table, but some legacy schemas
        # instead store the code in a column named ``subject`` or reference
        # the subject table via a foreign key ``subject_id``.  Inspect the
        # Evaluation table to choose the appropriate filter.
        c = conn.cursor()
        c.execute("PRAGMA table_info(Evaluation)")
        eval_cols = [row[1] for row in c.fetchall()]
        # Default: expect a ``subject_code`` column
        eval_subject_col: Optional[str] = None
        join_subject = False
        if "subject_code" in eval_cols:
            eval_subject_col = "subject_code"
        elif "subject" in eval_cols:
            eval_subject_col = "subject"
        elif "subject_id" in eval_cols:
            # We'll need to join Subject on id
            eval_subject_col = "subject_id"
            join_subject = True

        # Build the query for evaluation results based on detected schema
        if eval_subject_col is None:
            raise sqlite3.OperationalError(
                "Evaluation table does not contain a subject reference column"
            )

        if not join_subject:
            # Simple filter on Evaluation table
            query = f"""
                SELECT
                    e.year AS year,
                    q.id AS question_id,
                    q.label AS question_label,
                    er.value AS value
                FROM EvaluationResult er
                JOIN Evaluation e ON e.id = er.evaluation_id
                JOIN Question q ON q.id = er.question_id
                WHERE e.{eval_subject_col} = ?
            """
            params = (subject_code,)
        else:
            # Join to Subject via subject_id and filter by subject.code
            query = f"""
                SELECT
                    e.year AS year,
                    q.id AS question_id,
                    q.label AS question_label,
                    er.value AS value
                FROM EvaluationResult er
                JOIN Evaluation e ON e.id = er.evaluation_id
                JOIN Question q ON q.id = er.question_id
                JOIN Subject s ON e.{eval_subject_col} = s.id
                WHERE s.id = ?
            """
            params = (subject_code,)

        df = pd.read_sql_query(query, conn, params=params)
        if df.empty:
            return pd.DataFrame()

        df["question"] = df["question_id"].astype(str) + " " + df["question_label"].astype(str)
        if columns is not None:
            cols = set(columns)
            df = df[df["question_id"].apply(lambda qid: any(qid.startswith(c) for c in cols))]
        table = df.pivot_table(
            index="year",
            columns="question",
            values="value",
            aggfunc="first",
        )
        table = table.reset_index().rename(columns={"year": "År"})

        # Append statistics if requested.  The schema of EvaluationStats varies between
        # installations; it may use columns like ``num_responses`` and ``num_invited``
        # or ``answered`` and ``invited`` and may include a precomputed
        # ``response_percent``.  Introspect the EvaluationStats table to determine
        # which column names to use.
        if include_stats:
            # Determine column names for responses and invited in EvaluationStats
            c.execute("PRAGMA table_info(EvaluationStats)")
            stat_cols = [row[1] for row in c.fetchall()]
            # Default names used in earlier schema
            resp_col = None
            invited_col = None
            percent_col = None
            # Identify columns: prefer numeric counts if available
            for name in stat_cols:
                lname = name.lower()
                if lname in {"num_responses", "responses", "answered"}:
                    resp_col = name
                elif lname in {"num_invited", "invited"}:
                    invited_col = name
                elif lname in {"response_percent", "response_percentage", "percent", "percent_response"}:
                    percent_col = name
            # Build base SELECT clause for stats query
            select_parts = ["e.year AS year"]
            if resp_col:
                select_parts.append(f"es.{resp_col} AS responses")
            if invited_col:
                select_parts.append(f"es.{invited_col} AS invited")
            if percent_col:
                select_parts.append(f"es.{percent_col} AS response_percent")
            # Construct the SQL query dynamically
            select_clause = ",\n                    ".join(select_parts)
            if not join_subject:
                stats_query = f"""
                    SELECT
                        {select_clause}
                    FROM EvaluationStats es
                    JOIN Evaluation e ON e.id = es.evaluation_id
                    WHERE e.{eval_subject_col} = ?
                """
                stats_params = (subject_code,)
            else:
                stats_query = f"""
                    SELECT
                        {select_clause}
                    FROM EvaluationStats es
                    JOIN Evaluation e ON e.id = es.evaluation_id
                    JOIN Subject s ON e.{eval_subject_col} = s.id
                    WHERE s.id = ?
                """
                stats_params = (subject_code,)
            stats = pd.read_sql_query(stats_query, conn, params=stats_params)
            if not stats.empty:
                # If multiple evaluations exist per year, aggregate numeric counts
                if "responses" in stats.columns or "invited" in stats.columns:
                    numeric_cols = []
                    if "responses" in stats.columns:
                        numeric_cols.append("responses")
                    if "invited" in stats.columns:
                        numeric_cols.append("invited")
                    # Sum numeric counts by year
                    stats_grouped = stats.groupby("year")[numeric_cols].sum().reset_index()
                else:
                    stats_grouped = stats.drop_duplicates(subset=["year"]).copy()

                # Compute percentages if possible
                stats_grouped.rename(columns={"year": "År"}, inplace=True)
                if "responses" in stats_grouped.columns and "invited" in stats_grouped.columns:
                    stats_grouped.rename(
                        columns={"responses": "Antall svar", "invited": "Antall invitert"},
                        inplace=True,
                    )
                    stats_grouped["Svar%"] = stats_grouped.apply(
                        lambda row: f"{round((row['Antall svar'] / row['Antall invitert'] * 100))} %"
                        if row["Antall invitert"] else "",
                        axis=1,
                    )
                elif percent_col and "response_percent" in stats.columns:
                    # Use the existing response_percent column directly
                    stats_grouped.rename(columns={"response_percent": "Svar%"}, inplace=True)
                # Merge into the table by year
                table = table.merge(stats_grouped, on="År", how="left")

        table.sort_values(by="År", ascending=True, inplace=True)
        cols = list(table.columns)
        if "År" in cols:
            cols.insert(0, cols.pop(cols.index("År")))
        table = table[cols]
        return table
    finally:
        conn.close()


def get_subjects_df(db_path: str) -> pd.DataFrame:
    """Return a DataFrame listing all subjects in the database.

    Each row contains two columns: ``id`` (the subject code) and ``name``
    (the descriptive name).  The result is sorted by subject code.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database file.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with columns ``id`` and ``name``.  If there are no
        subjects, returns an empty DataFrame.
    """
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT id, name FROM Subject ORDER BY id", conn
        )
        return df
    finally:
        conn.close()

# New function mirroring the subject list API from the 57e7293163fa60c343cc733a9d1b228ac5baa5d9
# commit.  This returns a list of dictionaries where each dictionary
# contains the subject code (``id``), optional name (``name``), the
# number of evaluations recorded for the subject (``evaluations``) and
# the range of years covered by those evaluations (``year_min`` and
# ``year_max``).  A simple search string can be provided to filter on
# subject code or name, and the number of returned records can be
# limited via ``limit``.  The implementation automatically adapts to
# variations in the schema: the ``Evaluation`` table may reference
# subjects via a ``subject_code``, ``subject`` or ``subject_id``
# column.  Missing years or evaluation counts are represented as
# ``None``.

def get_subjects(
    db_path: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """Return a list of subjects with evaluation counts and year range.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database file.
    search: str, optional
        A search string used to filter subjects.  If provided, it
        matches case‑insensitively against both the subject code and
        subject name.  When ``None`` no filtering is applied.
    limit: int, optional
        The maximum number of subjects to return.  If ``None`` or
        non‑positive, all matching subjects are returned.

    Returns
    -------
    list of dict
        A list of dictionaries, each with keys ``id``, ``name``,
        ``evaluations``, ``year_min`` and ``year_max``.  The
        ``evaluations`` key holds the number of distinct evaluation
        records for the subject; ``year_min`` and ``year_max``
        represent the minimum and maximum year found in the
        ``Evaluation`` table for that subject.  If there are no
        evaluations the numeric fields will be ``0`` or ``None``.
    """
    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        # Inspect the Evaluation table to determine how subjects are referenced.
        c.execute("PRAGMA table_info(Evaluation)")
        eval_cols = [row[1] for row in c.fetchall()]
        subject_col: Optional[str] = None
        # In new schemas the evaluation table stores the subject code directly
        if "subject_code" in eval_cols:
            subject_col = "subject_code"
        # Some older schemas use a column named ``subject``
        elif "subject" in eval_cols:
            subject_col = "subject"
        # Legacy schemas use ``subject_id`` with a foreign key to Subject.id
        elif "subject_id" in eval_cols:
            subject_col = "subject_id"
        else:
            raise sqlite3.OperationalError(
                "Evaluation table does not contain a subject reference column"
            )

        # Build the base SQL.  We always join Subject to Evaluation via the
        # detected subject column.  The COALESCE ensures name is a string.
        base_sql = f"""
            SELECT s.id,
                   COALESCE(s.name, '') AS name,
                   COUNT(DISTINCT e.id) AS evaluations,
                   MIN(e.year) AS year_min,
                   MAX(e.year) AS year_max
            FROM Subject s
            LEFT JOIN Evaluation e ON e.{subject_col} = s.id
        """
        where_clauses: list[str] = []
        params: list = []
        if search:
            # Use wildcard matching on both id and name
            where_clauses.append("(s.id LIKE ? OR s.name LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like])
        sql = base_sql
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " GROUP BY s.id ORDER BY s.id"
        if limit and isinstance(limit, int) and limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = c.execute(sql, params).fetchall()
        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "id": r[0],
                    "name": r[1],
                    "evaluations": int(r[2]) if r[2] is not None else 0,
                    "year_min": int(r[3]) if r[3] is not None else None,
                    "year_max": int(r[4]) if r[4] is not None else None,
                }
            )
        return result
    finally:
        conn.close()