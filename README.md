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

## Notas

- No añadimos frameworks extra (por ejemplo Flask) para mantener `requirements.txt` mínimo; el health endpoint utiliza `http.server` de la stdlib.
- `psycopg2-binary` se usa para facilitar la instalación en Windows; en producción prefiera `psycopg2` compilado.
- Si su proyecto está en otra carpeta, ajuste rutas de `.env` o copie `.env` al path esperado.

