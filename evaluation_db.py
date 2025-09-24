import sqlite3
import pandas as pd
from typing import Optional


def init_db(db_path: str) -> None:
    """Create the evaluation database with required tables."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Create tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Subject (
        id TEXT PRIMARY KEY,
        name TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Question (
        id TEXT PRIMARY KEY,
        label TEXT,
        display_order INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Evaluation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id TEXT,
        year INTEGER,
        term TEXT,
        FOREIGN KEY(subject_id) REFERENCES Subject(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS EvaluationResult (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        evaluation_id INTEGER,
        question_id TEXT,
        value REAL,
        FOREIGN KEY(evaluation_id) REFERENCES Evaluation(id),
        FOREIGN KEY(question_id) REFERENCES Question(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS EvaluationStats (
        evaluation_id INTEGER PRIMARY KEY,
        answered INTEGER,
        invited INTEGER,
        response_percent TEXT,
        FOREIGN KEY(evaluation_id) REFERENCES Evaluation(id)
    );
    """)
    conn.commit()
    conn.close()


def import_evaluations(db_path: str, csv_file: str, year: int, term: str) -> None:
    """Import evaluation data from a CSV file.

    The CSV file is expected to have columns:
    'Emne', '1.1 Forventninger', '1.2 Struktur og organisering', ..., '2. Alt i alt',
    'Antall svar', 'Antall invitert', 'Svar%'.
    """
    # Ensure the database has the necessary tables
    # Calling init_db here is idempotent: it will create tables if they do not exist.
    init_db(db_path)

    # Read CSV file (semicolon delimiter used in Excel Norwegian environment)
    df = pd.read_csv(csv_file, sep=';', decimal=',')

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Standard question IDs and labels
    question_ids = [
        '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '2'
    ]
    question_labels = [
        'Forventninger', 'Struktur og organisering', 'Forelesninger',
        'Andre læringsaktiviteter', 'Oppfølging, veiledning og tilbakemelding',
        'Opplevd læring', 'Alt i alt'
    ]
    # Insert questions if not exist
    for qid, label, order in zip(question_ids, question_labels, range(1, len(question_ids)+1)):
        cur.execute(
            "INSERT OR IGNORE INTO Question(id, label, display_order) VALUES (?,?,?);",
            (qid, label, order)
        )

    # Determine the column names for answered and invited responses. Some files use
    # "Antall invitert" while others use "Antall inviterte". Similarly for
    # answered and response percent.
    col_lower = [c.lower() for c in df.columns]
    # find answered column
    answer_col = None
    for c in df.columns:
        if 'antall svar' in c.lower():
            answer_col = c
            break
    # find invited column
    invited_col = None
    for c in df.columns:
        if 'antall invitert' in c.lower() or 'antall inviterte' in c.lower():
            invited_col = c
            break
    # find response percent column (optional)
    percent_col = None
    for c in df.columns:
        if 'svar%' in c.lower() or 'svarprosent' in c.lower():
            percent_col = c
            break
    # Process each row
    for _, row in df.iterrows():
        subject_id = str(row['Emne']).strip()
        # Insert subject if not exists
        cur.execute("INSERT OR IGNORE INTO Subject(id, name) VALUES (?, ?);", (subject_id, None))
        # Create evaluation
        cur.execute(
            "INSERT INTO Evaluation(subject_id, year, term) VALUES (?,?,?);",
            (subject_id, year, term)
        )
        evaluation_id = cur.lastrowid
        # Insert results
        for qid in question_ids:
            col_name = f"{qid} {dict(zip(question_ids, question_labels))[qid]}"
            value = row.get(col_name, None)
            if pd.isnull(value) or value is None:
                continue
            # Skip non-numeric values such as 'ikke relevant'
            if isinstance(value, str) and value.strip().lower() in ('ikke relevant', 'ikke-relevant', 'n/a', 'na'):
                continue
            # Convert to float, handling comma decimals
            try:
                if isinstance(value, str):
                    val_str = value.strip().replace('.', '').replace(',', '.') if value.count(',') == 1 and value.count('.') > 1 else value.strip().replace(',', '.')
                    numeric_value = float(val_str)
                else:
                    numeric_value = float(value)
            except Exception:
                continue
            cur.execute(
                "INSERT INTO EvaluationResult(evaluation_id, question_id, value) VALUES (?,?,?);",
                (evaluation_id, qid, numeric_value)
            )
        # Insert stats
        answered = None
        invited = None
        response_percent = None
        if answer_col and not pd.isnull(row.get(answer_col, None)):
            try:
                val = row[answer_col]
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    answered = int(val)
                else:
                    answered = int(str(val).replace(' ', '').replace(',', '.').strip())
            except Exception:
                answered = None
        if invited_col and not pd.isnull(row.get(invited_col, None)):
            try:
                val = row[invited_col]
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    invited = int(val)
                else:
                    invited = int(str(val).replace(' ', '').replace(',', '.').strip())
            except Exception:
                invited = None
        # store original percent if present
        if percent_col and not pd.isnull(row.get(percent_col, None)):
            response_percent = str(row[percent_col]).strip()
        cur.execute(
            "INSERT INTO EvaluationStats(evaluation_id, answered, invited, response_percent) VALUES (?,?,?,?);",
            (evaluation_id, answered, invited, response_percent)
        )

    conn.commit()
    conn.close()


def export_evaluations_to_html(db_path: str, year: int, term: str, output_html: str) -> None:
    """Export evaluations for a given year and term to an HTML table.

    This function keeps the older behaviour of exporting all standard questions
    and statistics. It still computes the response percent from the stored
    `response_percent` field for backwards compatibility.
    """
    conn = sqlite3.connect(db_path)
    # Query data for each evaluation in the given year/term
    # We build the query dynamically to pick up whatever questions are present
    df = pd.read_sql_query(
        """
        SELECT s.id AS Emne,
               q.id AS question_id,
               q.label AS question_label,
               er.value AS value,
               es.answered AS answered,
               es.invited AS invited,
               es.response_percent AS response_percent
        FROM Evaluation e
        JOIN Subject s ON e.subject_id = s.id
        LEFT JOIN EvaluationResult er ON e.id = er.evaluation_id
        LEFT JOIN Question q ON er.question_id = q.id
        LEFT JOIN EvaluationStats es ON e.id = es.evaluation_id
        WHERE e.year = ? AND e.term = ?
        ORDER BY s.id, q.display_order
        """,
        conn,
        params=(year, term),
    )
    conn.close()
    if df.empty:
        # no data available
        pd.DataFrame().to_html(output_html, index=False)
        return
    # Compute response percentage from answered and invited if possible
    def compute_resp(row):
        ans = row.get("answered")
        inv = row.get("invited")
        if pd.notnull(ans) and pd.notnull(inv) and inv:
            return f"{round((ans / inv) * 100):d} %"
        return row.get("response_percent", "")

    df["Svar%"] = df.apply(compute_resp, axis=1)
    # Pivot to wide format: one row per subject
    pivot_df = df.pivot_table(
        index="Emne",
        columns="question_label",
        values="value",
        aggfunc="max",
    )
    # Bring back answered/invited/Svar% as separate columns (use first non-null per subject)
    stats = (
        df.groupby("Emne")[["answered", "invited", "Svar%"]]
        .first()
        .rename(columns={"answered": "Antall svar", "invited": "Antall invitert"})
    )
    result_df = pivot_df.join(stats)
    # Order columns: questions first by display order, then stats
    # Determine question display order from Question table via labels present in pivot_df
    # For this simple export we rely on the alphabetical order of labels, which works reasonably.
    # The 'Svar%' column is included as part of stats.
    result_df.reset_index(inplace=True)
    # Write to HTML
    result_df.to_html(output_html, index=False, na_rep="", float_format=lambda x: f"{x:.2f}")


def export_subject_overview_to_html(
    db_path: str,
    subject_id: str,
    output_html: str,
    include_stats: bool = True,
    question_ids: Optional[list] = None,
) -> None:
    """Export evaluations for a single subject across years to an HTML table.

    The output table will have one row per year and columns for each question that
    appears in the data for the given subject. Only questions that appear in
    the evaluation results will be included, unless a specific list of
    `question_ids` is provided. Statistics (number of responses, invited and
    calculated response percent) can be included via the `include_stats` flag.

    Args:
        db_path: Path to the SQLite database.
        subject_id: Emnekode for which to fetch evaluations.
        output_html: Path to the HTML file to be written.
        include_stats: Whether to include columns for answered, invited and
            calculated response percent.
        question_ids: Optional list of question IDs to include; if None, all
            questions found for the subject are used.
    """
    conn = sqlite3.connect(db_path)
    # Read evaluation data for the subject across all years/terms
    df = pd.read_sql_query(
        """
        SELECT e.year AS year,
               e.term AS term,
               q.id   AS question_id,
               q.label AS question_label,
               er.value AS value,
               es.answered AS answered,
               es.invited AS invited
        FROM Evaluation e
        JOIN Subject s ON e.subject_id = s.id
        LEFT JOIN EvaluationResult er ON e.id = er.evaluation_id
        LEFT JOIN Question q ON er.question_id = q.id
        LEFT JOIN EvaluationStats es ON e.id = es.evaluation_id
        WHERE s.id = ?
        ORDER BY e.year DESC, q.display_order
        """,
        conn,
        params=(subject_id,),
    )
    conn.close()
    if df.empty:
        pd.DataFrame().to_html(output_html, index=False)
        return
    # Filter questions if a subset is provided
    if question_ids is not None:
        df = df[df["question_id"].isin(question_ids)]
        if df.empty:
            pd.DataFrame().to_html(output_html, index=False)
            return
    # Compute response percent from answered and invited
    df["Svar%"] = df.apply(
        lambda row: f"{round((row.answered / row.invited) * 100):d} %"
        if pd.notnull(row.answered) and pd.notnull(row.invited) and row.invited
        else "",
        axis=1,
    )
    # Identify all years
    years = sorted(df["year"].unique())
    # Determine the list of question labels (in order of appearance)
    question_order = []
    for _, subdf in df.sort_values(["year", "question_id"]).groupby("question_id"):
        label = subdf["question_label"].iloc[0]
        if label not in question_order:
            question_order.append(label)
    # Build a result DataFrame: rows by year, columns by question label
    result_rows = []
    for y in years:
        row_df = df[df["year"] == y]
        row_data = {"År": y}
        for q_label in question_order:
            # value for this question in this year
            val = row_df.loc[row_df["question_label"] == q_label, "value"]
            if not val.empty:
                row_data[q_label] = val.iloc[0]
            else:
                row_data[q_label] = None
        if include_stats:
            # Stats: use the first (since there may be multiple terms) for answered/invited
            first = row_df.iloc[0]
            row_data["Antall svar"] = int(first.answered) if pd.notnull(first.answered) else None
            row_data["Antall invitert"] = int(first.invited) if pd.notnull(first.invited) else None
            row_data["Svar%"] = (f"{round((first.answered / first.invited) * 100):d} %"
                                   if pd.notnull(first.answered) and pd.notnull(first.invited) and first.invited
                                   else "")
        result_rows.append(row_data)
    result_df = pd.DataFrame(result_rows)
    # Write to HTML
    result_df.to_html(output_html, index=False, na_rep="", float_format=lambda x: f"{x:.2f}")


def get_subject_overview_df(
    db_path: str,
    subject_id: str,
    include_stats: bool = True,
    question_ids: Optional[list] = None,
) -> pd.DataFrame:
    """Return a DataFrame summarising evaluations for a single subject across years.

    This function mirrors `export_subject_overview_to_html` but returns a
    pandas DataFrame rather than writing HTML to disk. It can be used by web
    APIs to generate dynamic output.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT e.year AS year,
               e.term AS term,
               q.id   AS question_id,
               q.label AS question_label,
               er.value AS value,
               es.answered AS answered,
               es.invited AS invited
        FROM Evaluation e
        JOIN Subject s ON e.subject_id = s.id
        LEFT JOIN EvaluationResult er ON e.id = er.evaluation_id
        LEFT JOIN Question q ON er.question_id = q.id
        LEFT JOIN EvaluationStats es ON e.id = es.evaluation_id
        WHERE s.id = ?
        ORDER BY e.year DESC, q.display_order
        """,
        conn,
        params=(subject_id,),
    )
    conn.close()
    if df.empty:
        return pd.DataFrame()
    if question_ids is not None:
        df = df[df["question_id"].isin(question_ids)]
        if df.empty:
            return pd.DataFrame()
    df["Svar%"] = df.apply(
        lambda row: f"{round((row.answered / row.invited) * 100):d} %"
        if pd.notnull(row.answered) and pd.notnull(row.invited) and row.invited
        else "",
        axis=1,
    )
    years = sorted(df["year"].unique())
    question_order = []
    for _, subdf in df.sort_values(["year", "question_id"]).groupby("question_id"):
        label = subdf["question_label"].iloc[0]
        if label not in question_order:
            question_order.append(label)
    result_rows = []
    for y in years:
        row_df = df[df["year"] == y]
        row_data = {"År": y}
        for q_label in question_order:
            val = row_df.loc[row_df["question_label"] == q_label, "value"]
            row_data[q_label] = val.iloc[0] if not val.empty else None
        if include_stats:
            first = row_df.iloc[0]
            row_data["Antall svar"] = int(first.answered) if pd.notnull(first.answered) else None
            row_data["Antall invitert"] = int(first.invited) if pd.notnull(first.invited) else None
            row_data["Svar%"] = (f"{round((first.answered / first.invited) * 100):d} %"
                                   if pd.notnull(first.answered) and pd.notnull(first.invited) and first.invited
                                   else "")
        result_rows.append(row_data)
    return pd.DataFrame(result_rows)



if __name__ == "__main__":
    # Example usage
    import sys
    if len(sys.argv) < 2:
        print("Usage: python evaluation_db.py <command> [args]")
        print("Commands:\n  init <db_path>\n  import <db_path> <csv_file> <year> <term>\n  export <db_path> <year> <term> <output_html>")
    else:
        cmd = sys.argv[1]
        if cmd == "init" and len(sys.argv) == 3:
            init_db(sys.argv[2])
        elif cmd == "import" and len(sys.argv) == 6:
            import_evaluations(sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5])
        elif cmd == "export" and len(sys.argv) == 6:
            export_evaluations_to_html(sys.argv[2], int(sys.argv[3]), sys.argv[4], sys.argv[5])
        else:
            print("Invalid command or arguments.")

def get_subjects(
    db_path: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Returnerer emner (Subject) som en liste med dicts:
    [
      {"id": "ECN170", "name": "Miljø- og ressursøkonomi", "evaluations": 6, "year_min": 2020, "year_max": 2025},
      ...
    ]
    Støtter enkel søkestreng mot id og name, og limit.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    base_sql = """
        SELECT s.id,
               COALESCE(s.name, '') AS name,
               COUNT(DISTINCT e.id) AS evaluations,
               MIN(e.year) AS year_min,
               MAX(e.year) AS year_max
        FROM Subject s
        LEFT JOIN Evaluation e ON e.subject_id = s.id
    """
    where = ""
    params: list = []
    if search:
        where = "WHERE s.id LIKE ? OR s.name LIKE ?"
        like = f"%{search}%"
        params.extend([like, like])

    group_order = " GROUP BY s.id ORDER BY s.id"
    if limit and isinstance(limit, int) and limit > 0:
        group_order += f" LIMIT {int(limit)}"

    sql = base_sql + (f" {where}" if where else "") + group_order
    rows = cur.execute(sql, params).fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "evaluations": int(r[2]) if r[2] is not None else 0,
            "year_min": int(r[3]) if r[3] is not None else None,
            "year_max": int(r[4]) if r[4] is not None else None,
        }
        for r in rows
    ]
