"""
Simple HTTP server to upload evaluation CSV files and import them into the database.

This server provides two endpoints:
  - GET /        – returns a basic HTML form where a CSV file, year and term can be specified.
  - POST /upload – accepts the uploaded file and parameters and invokes evaluation_db.import_evaluations.

Example run:
    python3 csv_upload_server.py --db /path/to/evaluations.db --port 7001

Note: This upload endpoint performs no authentication. If the server is exposed
externally, you should restrict access (e.g. via nginx basic auth or firewall).
"""

import argparse
import http.server
import socketserver
import cgi
import tempfile
import os
import evaluation_db


class CSVUploadHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for uploading CSV files."""

    def _render_form(self, message: str = "") -> None:
        """Send a simple HTML form to the client."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        form_html = f"""
        <html><head><title>Upload evaluation CSV</title></head>
        <body>
        <h2>Importer emneevaluering</h2>
        <p>{message}</p>
        <form enctype="multipart/form-data" method="post" action="/upload/">
            <label for="file">CSV-fil:</label> <input type="file" name="file" accept=".csv" required><br><br>
            <label for="year">År:</label> <input type="number" name="year" min="1900" max="2100" required><br><br>
            <label for="term">Termin:</label>
            <select name="term" required>
                <option value="vår">vår</option>
                <option value="høst">høst</option>
                <option value="augustblokk">augustblokk</option>
                <option value="januarblokk">januarblokk</option>
                <option value="juniblokk">juniblokk</option>
            </select><br><br>
            <input type="submit" value="Importer">
        </form>
        </body></html>
        """
        self.wfile.write(form_html.encode('utf-8'))

    def do_GET(self) -> None:
        # Show the form on '/' or '/upload' endpoints
        if self.path in ('/', '/upload', '/upload/'):
            self._render_form()
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'Not found')

    def do_POST(self) -> None:
        # Accept both '/upload' and '/upload/' as valid endpoints
        if not (self.path == '/upload' or self.path == '/upload/'):
            self.send_response(404)
            self.end_headers()
            return
        ctype, pdict = cgi.parse_header(self.headers.get('Content-Type'))
        if ctype != 'multipart/form-data':
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Bad request: expected multipart/form-data')
            return
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers['Content-Type']},
        )
        fileitem = form['file'] if 'file' in form else None
        year = form.getfirst('year')
        term = form.getfirst('term')
        # FieldStorage does not support truth-value testing; explicitly check None and attributes
        if (
            fileitem is None
            or not hasattr(fileitem, 'file')
            or fileitem.file is None
            or not year
            or not term
        ):
            self._render_form(message='Alle felter må fylles ut')
            return
        try:
            year_int = int(year)
        except ValueError:
            self._render_form(message='År må være et heltall')
            return
        # save uploaded file to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            tmp.write(fileitem.file.read())
            tmp_path = tmp.name
        try:
            evaluation_db.import_evaluations(self.server.db_path, tmp_path, year_int, term)
            message = f'Import fullført for {os.path.basename(fileitem.filename)}.'
            # log success to stderr for debugging
            print(f'Successfully imported {fileitem.filename} for {year_int} {term}')
        except Exception as exc:
            message = f'Import feilet: {exc}'
            print(f'Import error: {exc}')
        finally:
            os.unlink(tmp_path)
        self._render_form(message=message)


def run_upload_server(db_path: str, host: str, port: int) -> None:
    class Handler(CSVUploadHandler):
        pass
    with socketserver.TCPServer((host, port), Handler) as httpd:
        httpd.db_path = db_path
        print(f'CSV upload server running on {host}:{port}, writing to {db_path}')
        httpd.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run CSV upload server for evaluations')
    parser.add_argument('--db', dest='db', required=True, help='Path to SQLite database file')
    parser.add_argument('--host', dest='host', default='0.0.0.0', help='Host to bind (default 0.0.0.0)')
    parser.add_argument('--port', dest='port', type=int, default=7001, help='Port to listen on (default 7001)')
    args = parser.parse_args()
    run_upload_server(args.db, args.host, args.port)


if __name__ == '__main__':
    main()
