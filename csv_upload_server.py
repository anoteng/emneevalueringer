"""
Enkel HTTP-server for å lime inn evalueringer fra Excel og importere dem til MariaDB/AOL.

Endepunkter:
  - GET  /        – viser HTML-skjema (år, semester, run, textarea)
  - POST /upload  – tar imot innlimt tabell og kaller evaluation_db_mysql.import_pasted_evaluations

Serveren gjør ingen autentisering; kjør den kun når du trenger den, og helst bundet til 127.0.0.1
og/eller bak SSH-tunnel eller brannmur.
"""

import argparse
import http.server
import socketserver
from urllib.parse import parse_qs

import evaluation_db_mysql


class PasteUploadHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for innlimt Excel-tabell."""

    def _render_form(self, message: str = "") -> None:
        """Send et enkelt HTML-skjema til klienten."""
        # Hent semestere fra DB for nedtrekk
        try:
            semesters = evaluation_db_mysql.get_semesters(self.server.db_name)
        except Exception as exc:
            semesters = []
            message = (message + "<br>" if message else "") + f"Feil ved henting av semester-liste: {exc}"

        options_html = ""
        for sem in semesters:
            # forventer felter: id, name, rank
            options_html += f"<option value='{sem['id']}'>{sem['name']}</option>"

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()

        form_html = f"""<!DOCTYPE html>
<html lang="nb">
<head>
  <meta charset="utf-8">
  <title>Importer emneevalueringer (innlimt fra Excel)</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 2em;
      max-width: 900px;
    }}
    textarea {{
      width: 100%;
      font-family: monospace;
    }}
    label {{
      display: block;
      margin-top: 1em;
      font-weight: bold;
    }}
    input[type=number], select {{
      padding: 0.3em;
      margin-top: 0.2em;
    }}
    button {{
      margin-top: 1em;
      padding: 0.5em 1.2em;
    }}
    .msg {{
      margin-bottom: 1em;
      color: #900;
    }}
  </style>
</head>
<body>
  <h2>Importer emneevaluering</h2>
  <p>Kopier tabellen fra Excel (inkludert header) og lim inn i feltet under.</p>
  {"<p class='msg'>" + message + "</p>" if message else ""}
  <form method="post" action="">
    <label for="year">År:</label>
    <input type="number" id="year" name="year" min="1900" max="2100" value="2025" required>

    <label for="semester_id">Semester:</label>
    <select id="semester_id" name="semester_id" required>
      {options_html}
    </select>

    <label for="run">Run (løpenummer, 1 hvis kun én gjennomføring):</label>
    <input type="number" id="run" name="run" value="1" min="1">

    <label for="data">Data (limt inn fra Excel):</label>
    <textarea id="data" name="data" rows="25"></textarea>

    <button type="submit">Importer</button>
  </form>
</body>
</html>
"""
        self.wfile.write(form_html.encode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/", "/upload", "/upload/"):
            self._render_form()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Not found")

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in ("/upload", "/upload/"):
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        form = parse_qs(body)

        def _get(name, default=None):
            values = form.get(name)
            if not values:
                return default
            return values[0]

        year_raw = _get("year")
        semester_raw = _get("semester_id")
        run_raw = _get("run", "1")
        data = _get("data", "")

        if not (year_raw and semester_raw and data.strip()):
            self._render_form(message="Alle felter (år, semester, data) må fylles ut.")
            return

        try:
            year = int(year_raw)
            semester_id = int(semester_raw)
            run = int(run_raw or "1")
        except ValueError:
            self._render_form(message="År, semester og run må være heltall.")
            return

        try:
            inserted = evaluation_db_mysql.import_pasted_evaluations(
                self.server.db_name,
                year=year,
                semester_id=semester_id,
                run=run,
                tsv_text=data,
            )
            message = f"Import fullført: opprettet {inserted} evaluering(er) for år {year}, semester_id={semester_id}, run={run}."
            print(message)
            self._render_form(message=message)
        except Exception as exc:
            msg = f"Import feilet: {exc}"
            print(msg)
            self._render_form(message=msg)


def run_upload_server(db_name: str, host: str, port: int) -> None:
    class Handler(PasteUploadHandler):
        pass

    with socketserver.TCPServer((host, port), Handler) as httpd:
        httpd.db_name = db_name
        print(f"Paste upload server running on {host}:{port}, writing to DB '{db_name}'")
        httpd.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run paste upload server for evaluations (MariaDB)")
    parser.add_argument("--db", dest="db", required=True, help="Database name (f.eks. 'aol')")
    parser.add_argument("--host", dest="host", default="127.0.0.1", help="Host to bind (default 127.0.0.1)")
    parser.add_argument("--port", dest="port", type=int, default=7001, help="Port to listen on (default 7001)")
    args = parser.parse_args()
    run_upload_server(args.db, args.host, args.port)


if __name__ == "__main__":
    main()