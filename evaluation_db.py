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
        # Query evaluation results
        # We retrieve year, question code, question label and value
        query = """
            SELECT
                e.year AS year,
                q.id AS question_id,
                q.label AS question_label,
                er.value AS value
            FROM EvaluationResult er
            JOIN Evaluation e ON e.id = er.evaluation_id
            JOIN Question q ON q.id = er.question_id
            WHERE e.subject_code = ?
        """
        df = pd.read_sql_query(query, conn, params=(subject_code,))
        if df.empty:
            # Return empty DataFrame if no data
            return pd.DataFrame()

        # Concatenate question id and label for column naming
        df["question"] = df["question_id"].astype(str) + " " + df["question_label"].astype(str)

        # Filter by requested columns if given
        if columns is not None:
            # Expand each question id to its prefix (e.g. '1.1') and filter
            cols = set(columns)
            df = df[df["question_id"].apply(lambda qid: any(qid.startswith(c) for c in cols))]

        # Pivot to wide format: index by year, columns by question, values by value
        table = df.pivot_table(
            index="year",
            columns="question",
            values="value",
            aggfunc="first",  # one value per year/question
        )
        # Reset index and rename to 'År'
        table = table.reset_index().rename(columns={"year": "År"})

        # Append stats columns
        if include_stats:
            stats_query = """
                SELECT
                    e.year AS year,
                    es.num_responses AS responses,
                    es.num_invited AS invited
                FROM EvaluationStats es
                JOIN Evaluation e ON e.id = es.evaluation_id
                WHERE e.subject_code = ?
            """
            stats = pd.read_sql_query(stats_query, conn, params=(subject_code,))
            # Aggregate in case there are multiple evaluations per year
            stats_grouped = stats.groupby("year").sum().reset_index()
            stats_grouped.rename(
                columns={"year": "År", "responses": "Antall svar", "invited": "Antall invitert"},
                inplace=True,
            )
            # Compute response percentage; avoid division by zero
            stats_grouped["Svar%"] = stats_grouped.apply(
                lambda row: f"{round((row['Antall svar'] / row['Antall invitert'] * 100))} %"
                if row["Antall invitert"] else "",
                axis=1,
            )
            # Merge with table on 'År'
            table = table.merge(stats_grouped, on="År", how="left")

        # Sort ascending by year
        table.sort_values(by="År", ascending=True, inplace=True)
        # Ensure consistent column order: 'År' first
        cols = list(table.columns)
        if "År" in cols:
            cols.insert(0, cols.pop(cols.index("År")))
        table = table[cols]
        return table
    finally:
        conn.close()