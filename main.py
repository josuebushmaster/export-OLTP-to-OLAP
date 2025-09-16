import argparse
import logging
import os
import signal
import subprocess
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


LOG = logging.getLogger('sync_main')


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/', '/health', '/healthz'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
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
