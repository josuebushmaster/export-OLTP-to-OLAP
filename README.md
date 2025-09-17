# Sync OLTP → OLAP

Instrucciones rápidas para Windows PowerShell.

## 1) Crear y activar virtualenv (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si `Activate.ps1` está bloqueado por la política de ejecución, puedes permitirlo (ejecutar como administrador):

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 2) Instalar dependencias

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 3) Variables de entorno (`.env`)

Crear un archivo `.env` en el root del repo (dos niveles arriba de `sync`) con variables necesarias, por ejemplo:

```text
OLTP_HOST=localhost
OLTP_USER=postgres
OLTP_PASSWORD=
OLTP_NAME=tu_base_oltp
OLTP_PORT=5432

OLAP_HOST=localhost
OLAP_USER=postgres
OLAP_PASSWORD=
OLAP_OLTPNAME=tu_base_olap
OLAP_PORT=5432
```

## 4) Ejecutar sincronización completa

```powershell
python sync_oltp_to_olap.py
```

## 5) Ejecutar worker (escucha notificaciones Postgres)

```powershell
python worker_sync.py
```

## Uso como punto de entrada (nuevo)

Ahora `main.py` actúa como punto de entrada con subcomandos:

- `python main.py web --port 8080` → levanta un endpoint de health (`/health`) en el puerto indicado. Railway provee la variable de entorno `PORT` automáticamente.
- `python main.py worker` → ejecuta el worker que escucha notificaciones de Postgres.
- `python main.py once` → ejecuta una sincronización completa una sola vez.

Ejemplos (PowerShell):

```powershell
python main.py --help
python main.py web --port 8080
python main.py worker
python main.py once
```

## Despliegue en Railway

1. Crea un repositorio en GitHub y sube este proyecto.
2. En Railway, crea un nuevo proyecto y conéctalo a tu repo de GitHub.
3. En los Environment Variables del proyecto en Railway, define las variables necesarias (`OLTP_*`, `OLAP_*`, etc.). Railway exportará `PORT` para el proceso web.
4. Railway detectará el `Procfile` y ejecutará `web: python main.py web --port $PORT`. Para correr el worker, añade un segundo service con el comando `python main.py worker`.

### Alta (deploy) paso a paso

Estos son los pasos recomendados para "dar de alta" (deploy) el proyecto en Railway u otra plataforma similar:

1. Crear un repositorio en GitHub y subir el proyecto (incluye `Procfile` y `requirements.txt`).
2. En Railway, crea un nuevo proyecto y conéctalo a tu repo de GitHub.
3. Define las variables de entorno necesarias en Railway (Settings → Environment Variables). Variables mínimas recomendadas:

	- `OLTP_HOST`, `OLTP_USER`, `OLTP_PASSWORD`, `OLTP_DBNAME`, `OLTP_PORT`
	- `OLAP_HOST`, `OLAP_USER`, `OLAP_PASSWORD`, `OLAP_OLTPNAME`, `OLAP_PORT`
	- `SYNC_TOKEN` (token secreto para proteger `/sync`)
	- Opcionales: `WORKER_HEARTBEAT_SECONDS`, `LOG_LEVEL`

4. Railway leerá el `Procfile`. Crea *dos services* en Railway:

	- Service `web`: comando `python main.py web --port $PORT` (Railway exporta la variable `PORT`).
	- Service `worker`: comando `python main.py worker`.

	Nota: en Railway cada service corre en su propio contenedor/instancia. Esto es lo esperado y deseable.

5. Despliega / Deploy (Railway reconstruirá el entorno e instalará dependencias).
6. Revisa logs de ambos services (Web y Worker) desde el panel de Railway para confirmar que:

	- El `web` escucha y responde en `/` o `/health`.
	- El `worker` se conecta a la BD y registra `worker heartbeat: alive` periódicamente.

7. Prueba los endpoints (desde tu terminal o Postman):

```powershell
Invoke-RestMethod https://<tu-web-url>/
Invoke-RestMethod https://<tu-web-url>/worker-status
```

Si todo OK, la respuesta de `/` será `OK` y `/worker-status` mostrará el estado del worker.

### Nota importante sobre `worker_status.json` en producción

El worker escribe `worker_status.json` en el filesystem local cuando hace heartbeat. Eso funciona bien en desarrollo local, pero en plataformas como Railway los servicios `web` y `worker` se ejecutan en contenedores separados: el archivo creado por el worker no estará disponible para el proceso `web` (cada servicio tiene su propio sistema de archivos efímero).

Recomendaciones:

- Si necesitas un estado centralizado accesible por el `web` y el `worker`, usa una de estas alternativas:
  - Escribir el `last_heartbeat` en una tabla de la base de datos `OLTP`/`OLAP` (ejemplo SQL abajo).
  - Usar un key-value compartido (Redis) o un servicio externo (Cloud Firestore, etc.).

- Ejemplo de tabla mínima SQL para heartbeats (Postgres):

```sql
CREATE TABLE IF NOT EXISTS worker_status (
  name text PRIMARY KEY,
  last_heartbeat bigint NOT NULL
);

-- actualizar (upsert)
INSERT INTO worker_status (name, last_heartbeat) VALUES ('default', extract(epoch from now())::bigint)
ON CONFLICT (name) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
```

- Si mantienes `worker_status.json` en producción, recuerda que sólo será visible para procesos que compartan el mismo contenedor (p. ej. si ejecutas web y worker juntos en la misma máquina/container).

### Pruebas y verificación tras el alta

1. Ver logs del worker: busca `worker heartbeat: alive`.
2. Ver logs del web: confirma respuestas 200 en `/` y `/worker-status`.
3. Si `/worker-status` indica `stale`, revisa conectividad del worker a la DB y permisos de escritura (o considera la alternativa DB/Redis).

### Buenas prácticas

- No ejecutes tareas largas directamente desde el proceso `web` en producción: puede agotar recursos o fallar por timeouts. Mejor encolar trabajos o delegarlos al `worker`.
- Protege el endpoint de trigger (`/sync`) con `SYNC_TOKEN` y no lo expongas públicamente sin autenticación.
- Configura alertas/monitoring (p. ej. alertas si `worker` deja de escribir heartbeats).

## Checklist previo al alta (deploy)

Antes de crear el proyecto en Railway (o subir a producción), verifica lo siguiente:

- Código y tests: todos los tests deben pasar localmente (`python -m unittest discover -s tests -p "test_*.py" -v`).
- `requirements.txt` actualizado y free of local-only packages.
- `Procfile` presente (ya incluido) con `web` y `worker` si necesitas ambos.
- Variables de entorno definidas y probadas en local `.env` (no subir `.env` al repo).
- Scripts de migración/SQL listos (si vas a crear tablas para heartbeats u otras necesidades).
- Logs y monitoring: decide dónde se almacenarán logs (Railway muestra logs por service).

## Comandos útiles (PowerShell) para pruebas locales y deploy

- Activar virtualenv y instalar deps:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

- Exportar variables de entorno temporalmente en la sesión (PowerShell):

```powershell
$env:OLTP_HOST = 'localhost'
$env:OLTP_USER = 'postgres'
$env:OLTP_PASSWORD = ''
$env:OLTP_DBNAME = 'mi_oltp'
$env:SYNC_TOKEN = 'mi_token_secreto'
```

- Probar endpoint `/sync` protegido con token (GET):

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/sync?table=ventas&op=insert&token=mi_token_secreto"
```

- Probar endpoint `/sync` protegido con token (POST JSON):

```powershell
$body = @{ table='ventas'; op='insert'; id='123'; token='mi_token_secreto' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8080/sync' -Body $body -ContentType 'application/json'
```

- Comprobar `worker_status.json` localmente:

```powershell
(Get-Content .\worker_status.json -Raw) | ConvertFrom-Json
# o sólo mostrar el campo timestamp
(Get-Content .\worker_status.json -Raw | ConvertFrom-Json).last_heartbeat
```

## Alternativa recomendada: almacenar heartbeats en la base de datos (opcional)

En entornos gestionados como Railway, el sistema de archivos es efímero y no compartido entre servicios; por eso recomendamos usar la base de datos para almacenar el `last_heartbeat` del worker en lugar de `worker_status.json`.

1) Crear tabla (Postgres):

```sql
CREATE TABLE IF NOT EXISTS worker_status (
  name text PRIMARY KEY,
  last_heartbeat bigint NOT NULL
);
```

1) Upsert desde el worker (ejemplo SQL):

```sql
INSERT INTO worker_status (name, last_heartbeat) VALUES ('default', extract(epoch from now())::bigint)
ON CONFLICT (name) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
```

1) Snippet Python (sugerido) para integrar en `worker_sync.py` en lugar de escribir `worker_status.json`:

```python
# ... dentro del bucle heartbeat del worker
if os.getenv('USE_DB_HEARTBEAT') == '1':
	try:
		with conn.cursor() as c:
			c.execute("""
INSERT INTO worker_status (name, last_heartbeat) VALUES ('default', %s)
ON CONFLICT (name) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat
""", (int(now),))
			# commit if not in autocommit mode
	except Exception:
		LOG.exception('No se pudo actualizar heartbeat en la BD')
else:
	# comportamiento actual: escribir worker_status.json
	_write_status(int(now))
```

1) Ventaja: el `web` puede leer `worker_status` consultando la misma base de datos y no depende de archivos locales.

## Mantenimiento y consideraciones finales

- No guardes credenciales en el repositorio. Usa variables de entorno en Railway y `.env` local excluido por `.gitignore`.
- Considera instrumentación y métricas (Prometheus, etc.) si necesitas monitoreo avanzado.
- Antes de pasar a producción, haz pruebas de carga y verifica que `sync_oltp_to_olap.py` es idempotente o maneja duplicados.

## Notas

- No añadimos frameworks extra (por ejemplo Flask) para mantener `requirements.txt` mínimo; el health endpoint utiliza `http.server` de la stdlib.
- `psycopg2-binary` se usa para facilitar la instalación en Windows; en producción prefiera `psycopg2` compilado.
- Si su proyecto está en otra carpeta, ajuste rutas de `.env` o copie `.env` al path esperado.

## Ejecutar tests

He incluido tests mínimos usando `unittest` que verifican el health endpoint y el endpoint `/worker-status`.

Para ejecutar los tests en PowerShell:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Los tests arrancan un servidor HTTP local temporal en `127.0.0.1:8008` y comprueban las respuestas.

