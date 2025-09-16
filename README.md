# Sync OLTP → OLAP

Instrucciones rápidas para Windows PowerShell.

1) Crear y activar virtualenv (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si `Activate.ps1` está bloqueado por la política de ejecución, puedes permitirlo (ejecutar como administrador):

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

2) Instalar dependencias:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

3) Crear un archivo `.env` en el root del repo (dos niveles arriba de `sync`) con variables necesarias, por ejemplo:

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

4) Ejecutar sincronización completa:

```powershell
python sync_oltp_to_olap.py
```

5) Ejecutar worker (escucha notificaciones postgres):

```powershell
python worker_sync.py
```

Notas:
- `psycopg2-binary` se usa para facilitar la instalación en Windows; en producción prefiera `psycopg2` compilado.
- Si su proyecto está en otra carpeta, ajuste rutas de `.env` o copie `.env` al path esperado.