"""
Lightweight HTTP server exposing course evaluation summaries as HTML.

This module uses Python's built-in `http.server` to avoid external
dependencies. It serves evaluation summaries on endpoints of the form:

    /api/subject/<subject_id>?include_stats=true&questions=1.1,1.2

The response is an HTML table containing a row per year and columns for each
question and (optionally) statistics. The server can be run via:

    python3 evaluation_api.py --db /path/to/evaluations.db --port 8000

It is intended to be reverse proxied behind nginx for production use.
"""

import argparse
import http.server
import socketserver
import urllib.parse
from typing import List, Optional
import evaluation_db
import json

class EvaluationRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler to serve evaluation data."""

    def do_GET(self) -> None:
        # Parse the path and query
        parsed = urllib.parse.urlparse(self.path)
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) >= 3 and path_parts[0] == 'api' and path_parts[1] == 'subject':
            subject_id = path_parts[2]
            query_params = urllib.parse.parse_qs(parsed.query)
            include_stats = query_params.get('include_stats', ['true'])[0].lower() not in ('false', '0', 'no')
            questions_param = query_params.get('questions', [None])[0]
            question_ids: Optional[List[str]] = None
            if questions_param:
                question_ids = [q.strip() for q in questions_param.split(',') if q.strip()]
            # Fetch DataFrame
            df = evaluation_db.get_subject_overview_df(
                self.server.db_path,
                subject_id,
                include_stats=include_stats,
                question_ids=question_ids,
            )
            if df.empty:
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'No data found for subject {subject_id}'.encode('utf-8'))
                return
            html = df.to_html(index=False, na_rep="", float_format=lambda x: f"{x:.2f}")
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
            # /api/subjects[?q=ECN&limit=50]
            if len(path_parts) >= 2 and path_parts[0] == 'api' and path_parts[1] == 'subjects':
                query_params = urllib.parse.parse_qs(parsed.query)
                search = query_params.get('q', [None])[0]
                limit_str = query_params.get('limit', [None])[0]
                limit = int(limit_str) if (limit_str and limit_str.isdigit()) else None

                subjects = evaluation_db.get_subjects(self.server.db_path, search=search, limit=limit)
                payload = json.dumps({"items": subjects}, ensure_ascii=False)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(payload.encode('utf-8'))
                return

        else:
            # Not found
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'Not found')


def run_server(db_path: str, host: str, port: int) -> None:
    """Run the HTTP server until interrupted."""
    class Handler(EvaluationRequestHandler):
        pass
    # Attach database path to the server instance for access in handler
    with socketserver.TCPServer((host, port), Handler) as httpd:
        httpd.db_path = db_path
        print(f'Serving evaluations on {host}:{port}, using DB {db_path}')
        httpd.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run the evaluation web API server')
    parser.add_argument('--db', dest='db', required=True, help='Path to SQLite database file')
    parser.add_argument('--host', dest='host', default='0.0.0.0', help='Host to bind (default 0.0.0.0)')
    parser.add_argument('--port', dest='port', type=int, default=8000, help='Port to listen on (default 8000)')
    args = parser.parse_args()
    run_server(args.db, args.host, args.port)


if __name__ == '__main__':
    main()
