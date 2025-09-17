import argparse
import logging
import os
import signal
import subprocess
import sys
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import argparse
import logging
import os
import signal
import subprocess
import sys
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs




LOG = logging.getLogger('sync_main')


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # health endpoints
        if self.path in ('/', '/health', '/healthz'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'OK')
            return

LOG = logging.getLogger('sync_main')


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # health endpoints
        if self.path in ('/', '/health', '/healthz'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'OK')
            return

        # worker status endpoint (reads worker_status.json)
        if self.path == '/worker-status':
            status = {'worker': 'unknown', 'last_heartbeat': None, 'age_seconds': None}
            status_file = os.path.join(os.path.dirname(__file__), 'worker_status.json')
            try:
                with open(status_file, 'r', encoding='utf-8') as fh:
                    data = json.load(fh)
                    last = float(data.get('last_heartbeat', 0))
                    status['last_heartbeat'] = last
                    status['age_seconds'] = int(time.time() - last) if last else None
                    status['worker'] = 'up' if last and (time.time() - last) < 120 else 'stale'
            except FileNotFoundError:
                status['worker'] = 'not_started'
            except Exception:
                status['worker'] = 'error'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(status).encode('utf-8'))
            return

        # sync trigger endpoint: /sync?table=...&op=...&id=...&token=...
        if self.path.startswith('/sync'):
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            token_env = os.getenv('SYNC_TOKEN')
            if token_env and qs.get('token', [None])[0] != token_env:
                self.send_response(403)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'forbidden'}).encode('utf-8'))
                return

            table = qs.get('table', [None])[0]
            op = qs.get('op', [None])[0]
            record_id = qs.get('id', [None])[0]

            script = os.path.join(os.path.dirname(__file__), 'sync_oltp_to_olap.py')
            cmd = [sys.executable, script]
            if table:
                cmd += ['--table', table]
            if op:
                cmd += ['--op', op]
            if record_id:
                cmd += ['--id', record_id]

            try:
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                resp = {
                    'returncode': proc.returncode,
                    'stdout': proc.stdout.splitlines()[-20:],
                    'stderr': proc.stderr.splitlines()[-20:],
                }
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
            return

        # default 404
        self.send_response(404)
        self.end_headers()


def run_health_server(host: str, port: int):
    server = HTTPServer((host, port), HealthHandler)
    LOG.info('Health server listening on %s:%d', host, port)

    def _stop(signum, frame):
        LOG.info('Stopping health server')
        try:
            server.shutdown()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    server.serve_forever()


def run_worker(python_path: str = sys.executable):
    # Lanza worker_sync.py en un subproceso y propaga la salida
    script = os.path.join(os.path.dirname(__file__), 'worker_sync.py')
    LOG.info('Iniciando worker: %s %s', python_path, script)
    proc = subprocess.Popen([python_path, script], stdout=sys.stdout, stderr=sys.stderr)

    def _handle(signum, frame):
        LOG.info('Terminando worker (signal=%s)', signum)
        try:
            proc.terminate()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    return proc.wait()


def run_once(python_path: str = sys.executable):
    # Lanza la sincronización completa una vez usando sync_oltp_to_olap.py
    script = os.path.join(os.path.dirname(__file__), 'sync_oltp_to_olap.py')
    LOG.info('Ejecutando sincronización única: %s %s', python_path, script)
    return subprocess.call([python_path, script])


def build_arg_parser():
    p = argparse.ArgumentParser(description='Punto de entrada para Sync OLTP → OLAP')
    sub = p.add_subparsers(dest='command', required=False)

    web = sub.add_parser('web', help='Levantar health endpoint (útil para Railway/hosting)')
    web.add_argument('--host', default=os.getenv('HOST', '0.0.0.0'))
    web.add_argument('--port', type=int, default=int(os.getenv('PORT', 8080)))

    worker = sub.add_parser('worker', help='Ejecutar worker que escucha notificaciones PG')

    once = sub.add_parser('once', help='Ejecutar una sincronización completa una vez')

    return p


def configure_logging():
    level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format='%(asctime)s %(levelname)s %(message)s')


def main(argv=None):
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # Si no se pasa comando, mostrar help y salir con código 0 para CI
    if not args.command:
        parser.print_help()
        return 0

    if args.command == 'web':
        run_health_server(args.host, args.port)
    elif args.command == 'worker':
        return run_worker()
    elif args.command == 'once':
        return run_once()
    else:
        parser.print_help()
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

