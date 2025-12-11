"""
evaluation_db_mysql.py
~~~~~~~~~~~~~~~~~~~~~~

MariaDB-backend for emneevalueringer, koblet mot AOL-databasen.

Forventer følgende tabeller:

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
import logging
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ---------------------------------------------------------------------------
# DB-tilkobling
# ---------------------------------------------------------------------------
def _norm_label(label: str) -> str:
    """Litt forsiktig normalisering av label for matching."""
    if label is None:
        return ""
    s = label.strip().lower()
    # fjern noen vanlige skilletegn på slutten
    while s and s[-1] in ".!?:":
        s = s[:-1]
    # slå sammen whitespace
    parts = s.split()
    return " ".join(parts)


def _get_engine(db_name: str):
    """
    Lag en SQLAlchemy-engine basert på miljøvariabler.

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

    # Escape brukernavn og passord for URL
    user = quote_plus(user)
    password = quote_plus(password)

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(
        url,
        pool_recycle=3600,
        pool_pre_ping=True,
    )
    return engine


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
    engine = _get_engine(db_path)

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

    params = {}

    if search:
        sql += """
            WHERE
              c.course_code LIKE :search
              OR c.name_no LIKE :search
              OR c.name_eng LIKE :search
        """
        params["search"] = f"%{search}%"

    sql += " GROUP BY c.course_code, name ORDER BY c.course_code"

    if limit is not None:
        # LIMIT kan ikke alltid parameteriseres pent, så vi interpolerer her etter int-cast
        sql += f" LIMIT {int(limit)}"

    df = pd.read_sql(text(sql), engine, params=params)

    if df.empty:
        return []

    result: list[dict] = []
    for _, r in df.iterrows():
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
    engine = _get_engine(db_path)

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
        JOIN courses c
          ON e.course_id = c.id
        WHERE c.course_code = :code
    """

    df = pd.read_sql(text(sql), engine, params={"code": subject_code})

    if df.empty:
        return pd.DataFrame()

    # Sørg for at value faktisk er numerisk, uansett hva DB/drivers gjør
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    if df.empty:
        return pd.DataFrame()

    # "1.1 Læringsutbytte"
    df["question"] = df["question_code"].astype(str) + " " + df["question_label"].astype(str)

    # Filtrer på spørsmålskoder om ønskelig
    if columns:
        wanted = list(columns)

        def _keep(code: str) -> bool:
            return any(str(code).startswith(prefix) for prefix in wanted)

        df = df[df["question_code"].apply(_keep)]

    if df.empty:
        return pd.DataFrame()

    # Pivot: rader = (year, term_name, semester_rank, run), kolonner = spørsmål
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
            JOIN courses c
              ON e.course_id = c.id
            WHERE c.course_code = :code
        """
        stats_df = pd.read_sql(text(stats_sql), engine, params={"code": subject_code})

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

# ---------------------------------------------------------------------------
# Import fra innlimt Excel/TSV (brukes av separat upload-server)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


def _parse_float(cell: str):
    """Parse norsk desimal med komma. Tom/«ikke relevant» -> None."""
    if cell is None:
        return None
    s = cell.strip()
    if not s:
        return None
    low = s.lower()
    if "ikke relevant" in low:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(cell: str):
    """Parse heltall (tåler evt. 3,0 osv.). Tom -> None."""
    if cell is None:
        return None
    s = cell.strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return int(float(s))
    except ValueError:
        return None


def get_semesters(db_path: str) -> list[dict]:
    """
    Hent liste over semester (id, name, rank), sortert på rank/id.

    Brukes av upload-serveren for å bygge nedtrekkslista.
    """
    engine = _get_engine(db_path)
    with engine.connect() as conn:
        res = conn.execute(
            text("SELECT id, name, rank FROM semester ORDER BY rank, id")
        )
        return [dict(row._mapping) for row in res]


def import_pasted_evaluations(
    db_name: str,
    year: int,
    semester_id: int,
    run: int,
    tsv_text: str,
) -> int:
    """
    Importer emneevalueringer fra innlimt Excel-tabell (TSV).

    Forventet format (kopiert fra Excel, inkl. header):

        Emne    1.1 Forventninger    1.2 Struktur og organisering   ...   2. Alt i alt   Antall svar   Antall inviterte

    - Første kolonne: Emne (courses.course_code)
    - Siste to kolonner: Antall svar, Antall inviterte
    - Mellom der: spørsmål, med "kode + label" i header (f.eks. "1.1 Forventninger")

    Verdier:
      - tall med komma -> float
      - tom celle / «ikke relevant» -> ingen rad i course_eval_result
    """
    lines = [ln for ln in tsv_text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("Ingen data å importere (tom innliming).")

    header = [c.strip() for c in lines[0].split("\t")]
    if not header or not header[0].lower().startswith("emne"):
        raise ValueError("Første kolonne i header må være 'Emne'.")

    def _find_col(prefix: str) -> int:
        prefix_low = prefix.lower()
        for i, col in enumerate(header):
            if col.lower().startswith(prefix_low):
                return i
        raise ValueError(f"Fant ikke kolonne som starter med '{prefix}' i headeren.")

    idx_answered = _find_col("antall svar")
    idx_invited = _find_col("antall inviter")

    # Spørsmålskolonner: alt mellom Emne og Antall svar
    question_cols = header[1:idx_answered]
    question_defs: list[tuple[str, str]] = []  # (code, label)

    for col in question_cols:
        if not col:
            continue
        raw = col.strip()

        # Først prøver vi varianten "1 - Veileders tilgjengelighet"
        num_part, sep, rest = raw.partition("-")
        if sep:  # vi fant en bindestrek
            code = num_part.strip().rstrip(".")  # "1" eller "1."
            label = rest.strip()  # "Veileders tilgjengelighet"
        else:
            # Fallback: "1.1 Forventninger"
            parts = raw.split(None, 1)
            if not parts:
                continue
            code = parts[0].rstrip(".")
            label = parts[1].strip() if len(parts) > 1 else ""

        if not code:
            continue

        question_defs.append((code, label))

    # Parse datalinjer
    data_rows: list[list[str]] = []
    course_codes: set[str] = set()

    for ln in lines[1:]:
        cells = ln.split("\t")
        if len(cells) < len(header):
            cells = cells + [""] * (len(header) - len(cells))
        cells = [c.strip() for c in cells]
        if not cells or not cells[0]:
            continue
        course_code = cells[0]
        course_codes.add(course_code)
        data_rows.append(cells)

    if not data_rows:
        raise ValueError("Ingen gyldige rader (mangler emnekoder).")

    engine = _get_engine(db_name)

    # Bruk én connection + transaksjon for hele importen
    with engine.begin() as conn:
        # 1) Map emnekoder -> course_id
        codes_list = sorted(course_codes)
        placeholders = ", ".join(
            f":code{i}" for i in range(len(codes_list))
        )
        params = {f"code{i}": code for i, code in enumerate(codes_list)}

        res = conn.execute(
            text(
                f"""
                SELECT id, course_code
                FROM courses
                WHERE course_code IN ({placeholders})
                """
            ),
            params,
        )
        rows = list(res)
        code_to_course_id = {r._mapping["course_code"]: r._mapping["id"] for r in rows}
        missing = sorted(course_codes - set(code_to_course_id.keys()))
        if missing:
            raise ValueError(
                "Følgende emnekoder finnes ikke i courses-tabellen: "
                + ", ".join(missing)
            )

        # 2) Eksisterende spørsmål – tillater flere rader med samme code,
        # men prøver å gjenbruke "samme" spørsmål (lik label) om mulig.
        res_q = conn.execute(
            text(
                "SELECT id, code, label, display_order "
                "FROM course_eval_question"
            )
        )
        existing_rows = [dict(r._mapping) for r in res_q]

        if existing_rows:
            next_display_order = max(
                (row["display_order"] or 0) for row in existing_rows
            ) + 1
        else:
            next_display_order = 1

        # Bygg opp to hjelpekart:
        #  - (code, norm_label) -> id  (eksakt match på både kode og "lik" tekst)
        #  - code -> liste av rader (om du senere vil gjøre mer fuzzy matching)
        existing_by_code_and_label: dict[tuple[str, str], int] = {}
        existing_by_code: dict[str, list[dict]] = {}

        for row in existing_rows:
            code = row["code"]
            label = row.get("label") or ""
            norm = _norm_label(label)
            existing_by_code_and_label[(code, norm)] = row["id"]
            existing_by_code.setdefault(code, []).append(row)

        code_to_question_id: dict[str, int] = {}

        # Sørg for at alle spørsmål i header finnes.
        # Strategi:
        #  1) Finn eksisterende rad med samme code + "samme" label -> gjenbruk
        #  2) Hvis ikke: opprett NY rad med samme code, ny label
        for code, label in question_defs:
            norm_label = _norm_label(label)
            key = (code, norm_label)

            if key in existing_by_code_and_label:
                # samme kode og (nesten) samme tekst -> bruk eksisterende id
                qid = existing_by_code_and_label[key]
                code_to_question_id[code] = qid
                continue

            # Ingen match på (code, label) -> opprett NYTT spørsmål
            conn.execute(
                text(
                    """
                    INSERT INTO course_eval_question (code, label, display_order)
                    VALUES (:code, :label, :display_order)
                    """
                ),
                {"code": code, "label": label, "display_order": next_display_order},
            )
            qid = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar_one()
            qid = int(qid)

            # Oppdater kartene slik at senere imports kan gjenbruke dette
            existing_by_code_and_label[key] = qid
            existing_by_code.setdefault(code, []).append(
                {
                    "id": qid,
                    "code": code,
                    "label": label,
                    "display_order": next_display_order,
                }
            )

            code_to_question_id[code] = qid
            next_display_order += 1

        inserted_evals = 0

        # 3) Importer rad for rad
        for cells in data_rows:
            course_code = cells[0]
            course_id = code_to_course_id[course_code]

            # Opprett evaluering
            conn.execute(
                text(
                    """
                    INSERT INTO course_eval (course_id, year, semester_id, run)
                    VALUES (:course_id, :year, :semester_id, :run)
                    """
                ),
                {
                    "course_id": course_id,
                    "year": year,
                    "semester_id": semester_id,
                    "run": run,
                },
            )
            eval_id = conn.execute(
                text("SELECT LAST_INSERT_ID()")
            ).scalar_one()
            eval_id = int(eval_id)

            # Stats
            answered = (
                _parse_int(cells[idx_answered])
                if idx_answered < len(cells) else None
            )
            invited = (
                _parse_int(cells[idx_invited])
                if idx_invited < len(cells) else None
            )
            resp_pct = None
            if answered is not None and invited not in (None, 0):
                resp_pct = round(100.0 * answered / invited * 100.0) / 100.0

            conn.execute(
                text(
                    """
                    INSERT INTO course_eval_stats (evaluation_id, answered, invited, response_percent)
                    VALUES (:evaluation_id, :answered, :invited, :response_percent)
                    """
                ),
                {
                    "evaluation_id": eval_id,
                    "answered": answered,
                    "invited": invited,
                    "response_percent": resp_pct,
                },
            )

            # Spørsmålsverdier
            for offset, (code, _label) in enumerate(question_defs, start=1):
                col_idx = 1 + offset - 1  # første spørsmålskolonne er index 1
                if col_idx >= len(cells):
                    continue
                val = _parse_float(cells[col_idx])
                if val is None:
                    # tom eller "ikke relevant" -> ingen rad
                    continue
                qid = code_to_question_id[code]
                conn.execute(
                    text(
                        """
                        INSERT INTO course_eval_result (evaluation_id, question_id, value)
                        VALUES (:evaluation_id, :question_id, :value)
                        """
                    ),
                    {
                        "evaluation_id": eval_id,
                        "question_id": qid,
                        "value": val,
                    },
                )

            inserted_evals += 1

    logger.info(
        "Importert %s evalueringer for år=%s semester_id=%s run=%s",
        inserted_evals,
        year,
        semester_id,
        run,
    )
    return inserted_evals
