import os
import sys
import select
import subprocess
from dotenv import load_dotenv
import psycopg2

# Cargar variables de entorno desde el root del repo
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Conexión simple y listener; la implementación original funcionaba correctamente anoche
conn = psycopg2.connect(
    host=os.getenv('OLTP_HOST', 'shortline.proxy.rlwy.net'),
    user=os.getenv('OLTP_USER', 'postgres'),
    password=os.getenv('OLTP_PASSWORD', ''),
    dbname=os.getenv('OLTP_DBNAME', 'railway'),
    port=int(os.getenv('OLTP_PORT', 39237))
)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Lista de tablas a escuchar
tablas = ["ventas", "productos", "clientes", "categoria", "orden", "orden_producto"]
for tabla in tablas:
    cur.execute(f"LISTEN {tabla}_sync;")

print("Esperando notificaciones de todas las tablas clave...")

while True:
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
        print(f"Notificación recibida | Tabla: {tabla} | Operación: {operacion} | ID: {id_registro}")
        cmd = [
            sys.executable, "infrastructure/sync/sync_oltp_to_olap.py",
            "--table", tabla,
            "--op", operacion,
        ]
        try:
            id_int = int(id_registro)
            cmd += ["--id", str(id_int)]
        except (TypeError, ValueError):
            pass
        subprocess.run(cmd)
