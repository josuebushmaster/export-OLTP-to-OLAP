import os
import sys
import select
import subprocess
import time
import signal
import logging
import json
from dotenv import load_dotenv
import psycopg2

# Cargar variables de entorno desde el root del repo
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

LOG = logging.getLogger('worker_sync')
LOG.setLevel(logging.INFO)
if not LOG.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    LOG.addHandler(ch)



# Intentar conectar al OLTP y preparar LISTEN; envolver en try/except para que
# cualquier fallo de conexión sea claramente visible en los logs (Railway).
try:
    LOG.info('Worker arrancando: conectando a OLTP...')
    conn = psycopg2.connect(
        host=os.getenv('OLTP_HOST', 'shortline.proxy.rlwy.net'),
        user=os.getenv('OLTP_USER', 'postgres'),
        password=os.getenv('OLTP_PASSWORD', ''),
        dbname=os.getenv('OLTP_DBNAME', 'railway'),
        port=int(os.getenv('OLTP_PORT', 39237))
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
except Exception:
    LOG.exception('Fallo al conectar a OLTP al arrancar el worker; saliendo')
    # Aseguramos que la excepción y salida queden en los logs de stdout
    sys.exit(1)

# Lista de tablas a escuchar
tablas = ["ventas", "productos", "clientes", "categoria", "orden", "orden_producto"]
for tabla in tablas:
    cur.execute(f"LISTEN {tabla}_sync;")

LOG.info("Esperando notificaciones de todas las tablas clave...")

running = True


def _shutdown(signum, frame):
    global running
    LOG.info("Recibido signal %s, cerrando worker...", signum)
    running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def _write_status(ts: int):
    try:
        status_file = os.path.join(os.path.dirname(__file__), 'worker_status.json')
        with open(status_file, 'w', encoding='utf-8') as fh:
            json.dump({'last_heartbeat': ts}, fh)
    except Exception:
        LOG.exception('No se pudo escribir worker_status.json')


def _run_loop():
    last_heartbeat = 0
    heartbeat_interval = int(os.getenv('WORKER_HEARTBEAT_SECONDS', '30'))
    while running:
        now = time.time()
        if now - last_heartbeat >= heartbeat_interval:
            LOG.info('worker heartbeat: alive')
            last_heartbeat = int(now)
            _write_status(last_heartbeat)

        # use select to wait for notifications
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            canal = notify.channel
            tabla = canal.replace('_sync', '')
            payload = notify.payload or ''
            if ':' in payload:
                operacion, id_registro = payload.split(':', 1)
            else:
                operacion, id_registro = 'unknown', payload
            LOG.info("Notificación recibida | Tabla: %s | Operación: %s | ID: %s", tabla, operacion, id_registro)
            # Ejecutar el script de sincronización ubicado en el mismo directorio
            script = os.path.join(os.path.dirname(__file__), 'sync_oltp_to_olap.py')
            cmd = [sys.executable, script, "--table", tabla, "--op", operacion]
            try:
                id_int = int(id_registro)
                cmd += ["--id", str(id_int)]
            except (TypeError, ValueError):
                pass
            # Registrar el comando que ejecutará el worker (usar INFO para que aparezca en Railway)
            LOG.info('Ejecutando comando de sync: %s', ' '.join(cmd))
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                LOG.exception('Error ejecutando sync script: %s', e)


try:
    _run_loop()
except Exception:
    LOG.exception('Worker terminado con excepción')
finally:
    try:
        cur.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    LOG.info('Worker finalizado')
