"""
evaluation_api.py
~~~~~~~~~~~~~~~~~~~

This module implements a simple HTTP API server for querying course evaluation
data stored in a SQLite database.  The server exposes an endpoint at
``/api/subject/<subject_id>`` which returns a table of evaluation results for
the given subject across multiple years.  By default the response is an
HTML document containing a rendered table.  The format can be changed via
the query parameter ``format``: ``format=json`` returns JSON and
``format=csv`` returns a CSV document.  The parameter ``include_stats``
controls whether the statistics columns (number of responses, number of
invitees and response percentage) are included; it accepts ``true`` or
``false`` (case insensitive) and defaults to ``true``.  The ``questions``
parameter accepts a comma‑separated list of question codes (e.g. ``1.1,1.2``)
to limit the columns included in the result.

This server depends on the companion module ``evaluation_db`` which
implements the database schema and provides a high‑level API for querying
evaluation data.  See that module for details on creating and populating
the database.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import numpy as np
import pandas as pd
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

try:
    # MariaDB-variant av evaluation_db
    from evaluation_db_mysql import (
        get_subject_overview_df,
        get_subjects_df,
        get_subjects,
        get_programmes,
        get_programme_courses,
        generate_programme_pptx,
    )
except Exception as exc:  # pragma: no cover - just informative logging
    logging.getLogger(__name__).warning(
        "Unable to import evaluation_db: %s. API will not function.", exc
    )


def _parse_bool(value: str, default: bool) -> bool:
    """Return a boolean from a string, with a default when empty.

    Accepts typical truthy and falsy values (true/false, yes/no, 1/0).
    Any other value results in the default being returned.
    """
    if value is None:
        return default
    value_lower = value.lower()
    if value_lower in {"true", "t", "yes", "y", "1"}:
        return True
    if value_lower in {"false", "f", "no", "n", "0"}:
        return False
    return default


class EvaluationRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the evaluation API."""

    def _set_headers(self, status: int, content_type: str) -> None:
        """Send a response status and headers."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        """Handle a GET request."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)

        # Handle listing all subjects.  This endpoint supports optional
        # search (``q``) and limit (``limit``) parameters to filter and
        # limit results.  The default response format is JSON to mirror
        # the behaviour in commit 57e7293163fa60c343cc733a9d1b228ac5baa5d9.
        # ``format`` can be ``json`` (default), ``csv`` or ``html``.
        if path in {"/api/subject", "/api/subject/", "/api/subjects"}:
            fmt = query_params.get("format", ["json"])[0].lower()
            search = query_params.get("q", [None])[0]
            limit_param = query_params.get("limit", [None])[0]
            limit: int | None = None
            if limit_param:
                try:
                    limit = int(limit_param)
                except Exception:
                    limit = None
            try:
                subjects = get_subjects(
                    self.server.db_path,
                    search=search,
                    limit=limit,
                )
            except Exception as exc:
                logging.getLogger(__name__).exception(
                    "Error retrieving subject list: %s", exc
                )
                self._set_headers(500, "text/plain; charset=utf-8")
                self.wfile.write(f"Error retrieving subject list: {exc}".encode("utf-8"))
                return
            # JSON format (default) returns a dictionary with "items"
            if fmt == "json":
                payload = json.dumps({"items": subjects}, ensure_ascii=False)
                self._set_headers(200, "application/json; charset=utf-8")
                self.wfile.write(payload.encode("utf-8"))
                return
            # CSV and HTML convert the list of dicts into a DataFrame first
            df = pd.DataFrame(subjects)
            if fmt == "csv":
                csv_data = df.to_csv(index=False, sep=";", lineterminator="\n", decimal=',')
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Language",
                                 "nb-NO")
                self.end_headers()
                self.wfile.write(csv_data.encode("utf-8"))
                return
            else:  # html
                html = df.to_html(index=False, escape=False, classes="dataframe")
                page = f"""<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>Subject List</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 2em; }}
    table.dataframe {{ border-collapse: collapse; width: 100%; }}
    table.dataframe th, table.dataframe td {{ border: 1px solid #ddd; padding: 8px; }}
    table.dataframe th {{ background-color: #f2f2f2; }}
  </style>
</head>
<body>
  <h2>Subject List</h2>
  {html}
</body>
</html>"""
                self._set_headers(200, "text/html; charset=utf-8")
                self.wfile.write(page.encode("utf-8"))
                return

        # Handle /api/subject/<subject_id>
        if path.startswith("/api/subject/"):
            parts = path.strip("/").split("/")
            # Expect at least two segments: ['api', 'subject', '<id>']
            if len(parts) >= 3:
                subject_id = parts[2]
                include_stats = _parse_bool(
                    query_params.get("include_stats", [None])[0], True
                )
                questions_param = query_params.get("questions", [None])[0]
                questions: list[str] | None
                if questions_param:
                    questions = [q.strip() for q in questions_param.split(",") if q.strip()]
                else:
                    questions = None
                fmt = query_params.get("format", ["html"])[0].lower()
                try:
                    df = get_subject_overview_df(
                        self.server.db_path, subject_id, include_stats=include_stats
                    )
                except Exception as exc:
                    logging.getLogger(__name__).exception(
                        "Error retrieving data for %s: %s", subject_id, exc
                    )
                    self._set_headers(500, "text/plain; charset=utf-8")
                    self.wfile.write(
                        f"Error retrieving data for {subject_id}: {exc}".encode("utf-8")
                    )
                    return
                # Limit to selected questions if provided
                if questions:
                    cols_to_keep: list[str] = [col for col in df.columns if col == "År"]
                    for q in questions:
                        matches = [c for c in df.columns if c.startswith(q + " ") or c == q]
                        cols_to_keep.extend(matches)
                    if include_stats:
                        cols_to_keep.extend(
                            [c for c in ["Antall svar", "Antall invitert", "Svar%"] if c in df.columns]
                        )
                    df = df[cols_to_keep]
                if fmt == "json":
                    # Gjør df "JSON-safe": sørg for at NaN/NaT/Inf blir None
                    df_safe = df.copy()

                    # 1) Konverter til object så pandas lar oss putte inn None
                    df_safe = df_safe.astype(object)

                    # 2) Erstatt problemverdier
                    df_safe = df_safe.where(pd.notnull(df_safe), None)
                    df_safe = df_safe.replace({np.nan: None, np.inf: None, -np.inf: None})

                    result_json = df_safe.to_dict(orient="records")
                    payload = json.dumps(result_json, ensure_ascii=False, allow_nan=False)

                    self._set_headers(200, "application/json; charset=utf-8")
                    self.wfile.write(payload.encode("utf-8"))
                    return

                elif fmt == "csv":
                    csv_data = df.to_csv(index=False, sep=";", lineterminator="\n", decimal=",")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/csv; charset=utf-8")
                    self.send_header("Content-Language",
                                     "nb-NO")
                    self.end_headers()
                    self.wfile.write(csv_data.encode("utf-8"))
                    return
                else:
                    html = df.to_html(index=False, escape=False, classes="dataframe")
                    page = f"""<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>Evaluations for {subject_id}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 2em; }}
    table.dataframe {{ border-collapse: collapse; width: 100%; }}
    table.dataframe th, table.dataframe td {{ border: 1px solid #ddd; padding: 8px; }}
    table.dataframe th {{ background-color: #f2f2f2; }}
  </style>
</head>
<body>
  <h2>Evaluations for {subject_id}</h2>
  {html}
</body>
</html>"""
                    self._set_headers(200, "text/html; charset=utf-8")
                    self.wfile.write(page.encode("utf-8"))
                    return
        # Handle listing all programmes: /api/programmes
        if path in {"/api/programmes", "/api/programmes/"}:
            try:
                programmes = get_programmes(self.server.db_path)
            except Exception as exc:
                logging.getLogger(__name__).exception(
                    "Error retrieving programmes: %s", exc
                )
                self._set_headers(500, "text/plain; charset=utf-8")
                self.wfile.write(f"Error retrieving programmes: {exc}".encode("utf-8"))
                return
            payload = json.dumps({"items": programmes}, ensure_ascii=False)
            self._set_headers(200, "application/json; charset=utf-8")
            self.wfile.write(payload.encode("utf-8"))
            return

        # Handle /api/programme/<code>/courses
        if path.startswith("/api/programme/") and "/courses" in path:
            parts = path.strip("/").split("/")
            # Expect: ['api', 'programme', '<code>', 'courses']
            if len(parts) >= 4 and parts[3] == "courses":
                programme_code = parts[2]
                semester_param = query_params.get("semester", [None])[0]
                semesters_param = query_params.get("semesters", [None])[0]

                semester: int | None = None
                semesters: list[int] | None = None

                if semester_param:
                    try:
                        semester = int(semester_param)
                    except ValueError:
                        pass

                if semesters_param:
                    try:
                        semesters = [int(s.strip()) for s in semesters_param.split(",") if s.strip()]
                    except ValueError:
                        pass

                try:
                    courses = get_programme_courses(
                        self.server.db_path,
                        programme_code,
                        semester=semester,
                        semesters=semesters,
                    )
                except Exception as exc:
                    logging.getLogger(__name__).exception(
                        "Error retrieving courses for %s: %s", programme_code, exc
                    )
                    self._set_headers(500, "text/plain; charset=utf-8")
                    self.wfile.write(
                        f"Error retrieving courses for {programme_code}: {exc}".encode("utf-8")
                    )
                    return

                payload = json.dumps({"items": courses}, ensure_ascii=False)
                self._set_headers(200, "application/json; charset=utf-8")
                self.wfile.write(payload.encode("utf-8"))
                return

        # Handle /api/programme/<code>/export/pptx
        if path.startswith("/api/programme/") and "/export/pptx" in path:
            parts = path.strip("/").split("/")
            # Expect: ['api', 'programme', '<code>', 'export', 'pptx']
            if len(parts) >= 5 and parts[3] == "export" and parts[4] == "pptx":
                programme_code = parts[2]
                semester_param = query_params.get("semester", [None])[0]
                semesters_param = query_params.get("semesters", [None])[0]

                semester: int | None = None
                semesters: list[int] | None = None

                if semester_param:
                    try:
                        semester = int(semester_param)
                    except ValueError:
                        pass

                if semesters_param:
                    try:
                        semesters = [int(s.strip()) for s in semesters_param.split(",") if s.strip()]
                    except ValueError:
                        pass

                # Hent programnavn for tittelslide
                programme_name = programme_code
                try:
                    progs = get_programmes(self.server.db_path)
                    match = [p for p in progs if p["code"] == programme_code]
                    if match:
                        programme_name = match[0]["name"]
                except Exception:
                    pass

                try:
                    pptx_bytes = generate_programme_pptx(
                        self.server.db_path,
                        programme_code,
                        programme_name=programme_name,
                        semester=semester,
                        semesters=semesters,
                    )
                except Exception as exc:
                    logging.getLogger(__name__).exception(
                        "Error generating PPTX for %s: %s", programme_code, exc
                    )
                    self._set_headers(500, "text/plain; charset=utf-8")
                    self.wfile.write(
                        f"Error generating PPTX for {programme_code}: {exc}".encode("utf-8")
                    )
                    return

                filename = f"{programme_code}_evalueringer.pptx"
                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                )
                self.send_header(
                    "Content-Disposition",
                    f'attachment; filename="{filename}"',
                )
                self.send_header("Content-Length", str(len(pptx_bytes)))
                self.end_headers()
                self.wfile.write(pptx_bytes)
                return

        # If path doesn't match, return 404
        self._set_headers(404, "text/plain; charset=utf-8")
        self.wfile.write(b"Not Found")


def run_server(db_path: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    """Start the HTTP server with the given database path, host and port."""
    server_address = (host, port)
    # Bind the db_path to the server instance for use in the handler
    class _EvaluationServer(HTTPServer):
        def __init__(self, server_address, RequestHandlerClass):
            super().__init__(server_address, RequestHandlerClass)
            self.db_path = db_path

    httpd = _EvaluationServer(server_address, EvaluationRequestHandler)
    logging.info("Starting evaluation API server at http://%s:%s", host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        logging.info("Evaluation API server stopped.")


def main(argv: list[str] | None = None) -> int:
    """Parse command line arguments and start the server."""
    parser = argparse.ArgumentParser(description="Run the evaluation API server.")
    parser.add_argument(
        "--db",
        dest="db_path",
        required=True,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address to listen on (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_server(args.db_path, args.host, args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())