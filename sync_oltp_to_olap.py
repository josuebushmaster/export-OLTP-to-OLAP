import psycopg2
import psycopg2.extras
from datetime import datetime
import os
from dotenv import load_dotenv
import traceback
import argparse
import logging

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

OLTP_CONFIG = {
    'host': os.getenv('OLTP_HOST', 'shortline.proxy.rlwy.net'),
    'user': os.getenv('OLTP_USER', 'postgres'),
    'password': os.getenv('OLTP_PASSWORD', ''),
    'dbname': os.getenv('OLTP_DBNAME', 'railway'),
    'port': int(os.getenv('OLTP_PORT', 39237)),
}

OLAP_CONFIG = {
    'host': os.getenv('OLAP_HOST', 'caboose.proxy.rlwy.net'),
    'user': os.getenv('OLAP_USER', 'postgres'),
    'password': os.getenv('OLAP_PASSWORD', ''),
    'dbname': os.getenv('OLAP_DBNAME', 'railway'),
    'port': int(os.getenv('OLAP_PORT', 5432)),
}

def get_pg_conn(config):
    return psycopg2.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        dbname=config['dbname'],
        port=config['port'],
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# Logger: escribe DEBUG en un fichero dentro del directorio sync, no imprime en stdout
logger = logging.getLogger('sync')
logger.setLevel(logging.DEBUG)
try:
    log_path = os.path.join(os.path.dirname(__file__), 'sync.log')
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    # Evita añadir múltiples handlers si el módulo se importa varias veces
    if not logger.handlers:
        logger.addHandler(fh)
except Exception:
    # En caso de fallo con el file handler, no rompemos el flujo
    pass

def upsert_dim_cliente(cur, cliente):
    logger.debug(f"upsert_dim_cliente: id_cliente={cliente.get('id_cliente')}")
    cur.execute('''
        INSERT INTO dim_cliente (id_cliente, nombre, apellido, edad, email, telefono, direccion, ciudad, pais)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_cliente) DO UPDATE SET
            nombre=EXCLUDED.nombre, apellido=EXCLUDED.apellido, edad=EXCLUDED.edad,
            email=EXCLUDED.email, telefono=EXCLUDED.telefono, direccion=EXCLUDED.direccion,
            ciudad=EXCLUDED.ciudad, pais=EXCLUDED.pais;
    ''', (
        cliente['id_cliente'], cliente['nombre'], cliente['apellido'], cliente['edad'],
        cliente['email'], cliente['telefono'], cliente['direccion'], cliente.get('ciudad_envio'), cliente.get('pais_envio')
    ))
    return cliente['id_cliente']

def upsert_dim_categoria(cur, categoria):
    logger.debug(f"upsert_dim_categoria: id_categoria={categoria.get('id_categoria')}")
    cur.execute('''
        INSERT INTO dim_categoria (id_categoria, nombre_categoria, descripcion)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_categoria) DO UPDATE SET
            nombre_categoria=EXCLUDED.nombre_categoria, descripcion=EXCLUDED.descripcion;
    ''', (
        categoria['id_categoria'], categoria['nombre_categoria'], categoria['descripcion']
    ))
    return categoria['id_categoria']

def upsert_dim_producto(cur, producto):
    logger.debug(f"upsert_dim_producto: id_producto={producto.get('id_producto')}")
    cur.execute('''
        INSERT INTO dim_producto (id_producto, nombre_producto, descripcion, precio, costo, id_categoria)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_producto) DO UPDATE SET
            nombre_producto=EXCLUDED.nombre_producto, descripcion=EXCLUDED.descripcion,
            precio=EXCLUDED.precio, costo=EXCLUDED.costo, id_categoria=EXCLUDED.id_categoria;
    ''', (
        producto['id_producto'], producto['nombre_producto'], producto['descripcion'],
        producto['precio'], producto['costo'], producto['id_categoria']
    ))
    return producto['id_producto']

def upsert_dim_tiempo(cur, fecha):
    # Normaliza fecha a tipo date (evita problemas si recibimos datetime con hora)
    if isinstance(fecha, datetime):
        fecha = fecha.date()
    # Primero intentamos obtener existing row por fecha (reduce secuencias y evita duplicados)
    try:
        cur.execute('SELECT id_tiempo, fecha FROM dim_tiempo WHERE fecha = CAST(%s AS date);', (fecha,))
        existing = cur.fetchone()
        if existing:
            logger.debug(f"upsert_dim_tiempo: encontrado existente id_tiempo={existing.get('id_tiempo')} fecha={existing.get('fecha')}")
            return existing['id_tiempo']

        # Si no existe, construimos campos derivados y tratamos de insertar
        anio = fecha.year
        mes = fecha.month
        dia = fecha.day
        trimestre = (fecha.month - 1) // 3 + 1
        semana = fecha.isocalendar()[1]
        logger.debug(f"upsert_dim_tiempo: intentando insertar fecha={fecha} anio={anio} mes={mes} dia={dia} semana={semana}")
        try:
            cur.execute('''
                INSERT INTO dim_tiempo (fecha, anio, mes, dia, trimestre, semana)
                VALUES (CAST(%s AS date), %s, %s, %s, %s, %s)
                RETURNING id_tiempo, fecha;
            ''', (fecha, anio, mes, dia, trimestre, semana))
            row = cur.fetchone()
            if row:
                logger.debug(f"upsert_dim_tiempo: insert result id_tiempo={row.get('id_tiempo')} fecha_guardada={row.get('fecha')}")
                return row['id_tiempo']
        except Exception as e:
            # Manejo de carreras: si otra transacción insertó la misma fecha, capturamos y hacemos SELECT
            from psycopg2 import errors
            if isinstance(e, errors.UniqueViolation) or 'unique' in str(e).lower():
                # rollback parcial en el cursor/conn y re-SELECT la fila
                try:
                    cur.connection.rollback()
                except Exception:
                    pass
                cur.execute('SELECT id_tiempo, fecha FROM dim_tiempo WHERE fecha = CAST(%s AS date);', (fecha,))
                row = cur.fetchone()
                if row:
                    logger.debug(f"upsert_dim_tiempo: race resolved, found id_tiempo={row.get('id_tiempo')}")
                    return row['id_tiempo']
            # Si no es unique violation, volver a lanzar para que el caller lo maneje/loguee
            logger.exception(f"upsert_dim_tiempo: error al insertar fecha={fecha}: {e}")
            raise

    except Exception as ex:
        # En caso de cualquier fallo, registrar y volver a lanzar para manejo arriba
        logger.exception(f"upsert_dim_tiempo: fallo inesperado buscando/insertando fecha={fecha}: {ex}")
        raise

def upsert_dim_metodo_pago(cur, metodo_pago):
    # UPSERT atómico con RETURNING para obtener el ID tanto en insert como en conflicto
    # Versión estable: acepta cadenas vacías ('') como método de pago válido (comportamiento original)
    cur.execute('''
        INSERT INTO dim_metodo_pago (metodo_pago)
        VALUES (%s)
        ON CONFLICT (metodo_pago)
        DO UPDATE SET metodo_pago = EXCLUDED.metodo_pago
        RETURNING id_metodo_pago;
    ''', (metodo_pago,))
    row = cur.fetchone()
    return row['id_metodo_pago'] if row else None

def upsert_dim_envio(cur, estado_envio, metodo_envio):
    # UPSERT atómico con RETURNING para obtener el ID tanto en insert como en conflicto
    # Versión estable: permite valores vacíos ('') para los campos, solo omite si ambos son None
    logger.debug(f"upsert_dim_envio: estado_envio={estado_envio} metodo_envio={metodo_envio}")
    cur.execute('''
        INSERT INTO dim_envio (estado_envio, metodo_envio)
        VALUES (%s, %s)
        ON CONFLICT (estado_envio, metodo_envio)
        DO UPDATE SET estado_envio = EXCLUDED.estado_envio, metodo_envio = EXCLUDED.metodo_envio
        RETURNING id_envio;
    ''', (estado_envio, metodo_envio))
    row = cur.fetchone()
    return row['id_envio'] if row else None

def upsert_hecho_ventas(cur, hecho):
    logger.debug(f"upsert_hecho_ventas: id_tiempo={hecho.get('id_tiempo')} id_cliente={hecho.get('id_cliente')} id_producto={hecho.get('id_producto')} cantidad={hecho.get('cantidad')}")
    cur.execute('''
        INSERT INTO hecho_ventas (
            id_tiempo, id_cliente, id_producto, id_categoria, id_metodo_pago, id_envio,
            cantidad, total_venta, costo_envio, margen
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_tiempo, id_cliente, id_producto, id_categoria, id_metodo_pago, id_envio)
        DO UPDATE SET
            cantidad = EXCLUDED.cantidad,
            total_venta = EXCLUDED.total_venta,
            costo_envio = EXCLUDED.costo_envio,
            margen = EXCLUDED.margen;
    ''', (
        hecho['id_tiempo'], hecho['id_cliente'], hecho['id_producto'], hecho['id_categoria'],
        hecho['id_metodo_pago'], hecho['id_envio'], hecho['cantidad'], hecho['total_venta'],
        hecho['costo_envio'], hecho['margen']
    ))


def _sync_clientes(oltp_cur, olap_cur, id_cliente=None):
    logger.info(f"_sync_clientes start id={id_cliente}")
    if id_cliente is None:
        oltp_cur.execute('''
            SELECT c.*, o.ciudad_envio, o.pais_envio
            FROM clientes c
            LEFT JOIN orden o ON c.id_cliente = o.id_cliente
        ''')
    else:
        oltp_cur.execute('''
            SELECT c.*, o.ciudad_envio, o.pais_envio
            FROM clientes c
            LEFT JOIN orden o ON c.id_cliente = o.id_cliente
            WHERE c.id_cliente = %s
        ''', (id_cliente,))
    clientes = oltp_cur.fetchall()
    for cliente in clientes:
        logger.debug(f"_sync_clientes: procesando cliente id={cliente.get('id_cliente')}")
        upsert_dim_cliente(olap_cur, cliente)


def _sync_categorias(oltp_cur, olap_cur, id_categoria=None):
    logger.info(f"_sync_categorias start id={id_categoria}")
    if id_categoria is None:
        oltp_cur.execute('SELECT * FROM categoria;')
    else:
        oltp_cur.execute('SELECT * FROM categoria WHERE id_categoria = %s;', (id_categoria,))
    categorias = oltp_cur.fetchall()
    for categoria in categorias:
        logger.debug(f"_sync_categorias: procesando categoria id={categoria.get('id_categoria')}")
        upsert_dim_categoria(olap_cur, categoria)


def _sync_productos(oltp_cur, olap_cur, id_producto=None):
    logger.info(f"_sync_productos start id={id_producto}")
    if id_producto is None:
        oltp_cur.execute('SELECT * FROM productos;')
    else:
        oltp_cur.execute('SELECT * FROM productos WHERE id_producto = %s;', (id_producto,))
    productos = oltp_cur.fetchall()
    for producto in productos:
        logger.debug(f"_sync_productos: procesando producto id={producto.get('id_producto')}")
        upsert_dim_producto(olap_cur, producto)


def _sync_ventas(oltp_cur, olap_cur, id_venta=None, id_orden=None):
    logger.info(f"_sync_ventas start id_venta={id_venta} id_orden={id_orden}")
    base_query = '''
        SELECT v.fecha_venta, o.id_cliente, op.id_producto, p.id_categoria, v.metodo_pago,
               o.estado_envio, o.metodo_envio, op.cantidad, op.precio_unitario, p.precio, p.costo, o.costo_envio
        FROM ventas v
        JOIN orden o ON v.id_orden = o.id_orden
        JOIN orden_producto op ON o.id_orden = op.id_orden
        JOIN productos p ON op.id_producto = p.id_producto
    '''
    params = []
    if id_venta is not None:
        query = base_query + ' WHERE v.id_venta = %s'
        params = [id_venta]
    elif id_orden is not None:
        query = base_query + ' WHERE o.id_orden = %s'
        params = [id_orden]
    else:
        query = base_query

    oltp_cur.execute(query, params)
    ventas = oltp_cur.fetchall()
    for venta in ventas:
        logger.debug(f"_sync_ventas: procesando venta fecha={venta.get('fecha_venta')} id_producto={venta.get('id_producto')} cantidad={venta.get('cantidad')}")
        fecha_venta = venta['fecha_venta']
        if not isinstance(fecha_venta, datetime):
            fecha_venta = datetime.strptime(str(fecha_venta), "%Y-%m-%d")
        id_tiempo = upsert_dim_tiempo(olap_cur, fecha_venta)
        id_cliente = venta['id_cliente']
        id_producto = venta['id_producto']
        id_categoria = venta['id_categoria']

        # Asegurar que las dimensiones relacionadas existan en OLAP en el orden correcto
        try:
            # Categoria
            try:
                oltp_cur.execute('SELECT * FROM categoria WHERE id_categoria = %s;', (id_categoria,))
                categoria_row = oltp_cur.fetchone()
                if categoria_row:
                    logger.debug(f"_sync_ventas: upserting categoria id={id_categoria}")
                    upsert_dim_categoria(olap_cur, categoria_row)
                else:
                    logger.warning(f"_sync_ventas: categoria id={id_categoria} no encontrada en OLTP; creando placeholder")
                    upsert_dim_categoria(olap_cur, {'id_categoria': id_categoria, 'nombre_categoria': None, 'descripcion': None})
            except Exception as e:
                logger.exception(f"_sync_ventas: fallo al asegurar dim_categoria id={id_categoria}: {e}")

            # Cliente
            try:
                oltp_cur.execute('SELECT * FROM clientes WHERE id_cliente = %s;', (id_cliente,))
                cliente_row = oltp_cur.fetchone()
                if cliente_row:
                    logger.debug(f"_sync_ventas: upserting cliente id={id_cliente}")
                    upsert_dim_cliente(olap_cur, cliente_row)
                else:
                    logger.warning(f"_sync_ventas: cliente id={id_cliente} no encontrado en OLTP; creando placeholder")
                    upsert_dim_cliente(olap_cur, {'id_cliente': id_cliente, 'nombre': None, 'apellido': None, 'edad': None, 'email': None, 'telefono': None, 'direccion': None, 'ciudad_envio': None, 'pais_envio': None})
            except Exception as e:
                logger.exception(f"_sync_ventas: fallo al asegurar dim_cliente id={id_cliente}: {e}")

            # Producto
            try:
                oltp_cur.execute('SELECT * FROM productos WHERE id_producto = %s;', (id_producto,))
                producto_row = oltp_cur.fetchone()
                if producto_row:
                    logger.debug(f"_sync_ventas: upserting producto id={id_producto}")
                    upsert_dim_producto(olap_cur, producto_row)
                else:
                    logger.warning(f"_sync_ventas: producto id={id_producto} no encontrado en OLTP; creando placeholder")
                    upsert_dim_producto(olap_cur, {'id_producto': id_producto, 'nombre_producto': None, 'descripcion': None, 'precio': None, 'costo': None, 'id_categoria': id_categoria})
            except Exception as e:
                logger.exception(f"_sync_ventas: fallo al asegurar dim_producto id={id_producto}: {e}")

        except Exception:
            # Capturamos cualquier error en la preparación de dimensiones y continuamos para que el flujo lo loguee
            logger.exception("_sync_ventas: error inesperado asegurando dimensiones relacionadas")
        id_metodo_pago = upsert_dim_metodo_pago(olap_cur, venta['metodo_pago'])
        id_envio = upsert_dim_envio(olap_cur, venta['estado_envio'], venta['metodo_envio'])
        total_venta = venta['cantidad'] * venta['precio_unitario']
        margen = (venta['precio_unitario'] - venta['costo']) * venta['cantidad']
        hecho = {
            'id_tiempo': id_tiempo,
            'id_cliente': id_cliente,
            'id_producto': id_producto,
            'id_categoria': id_categoria,
            'id_metodo_pago': id_metodo_pago,
            'id_envio': id_envio,
            'cantidad': venta['cantidad'],
            'total_venta': total_venta,
            'costo_envio': venta['costo_envio'],
            'margen': margen
        }
        if all([id_tiempo, id_cliente, id_producto, id_categoria, id_metodo_pago, id_envio]):
            upsert_hecho_ventas(olap_cur, hecho)
        else:
            logger.warning(f"_sync_ventas: venta omitida por falta de dimensión: {hecho}")


def sync_all(oltp_cur, olap_cur):
    print('Sincronizando clientes...')
    _sync_clientes(oltp_cur, olap_cur)
    print('Sincronizando categorias...')
    _sync_categorias(oltp_cur, olap_cur)
    print('Sincronizando productos...')
    _sync_productos(oltp_cur, olap_cur)
    print('Sincronizando hechos de ventas...')
    _sync_ventas(oltp_cur, olap_cur)


def sync_oltp_to_olap(table: str | None = None, operation: str | None = None, record_id: int | None = None):
    oltp_conn = get_pg_conn(OLTP_CONFIG)
    olap_conn = get_pg_conn(OLAP_CONFIG)
    try:
        oltp_cur = oltp_conn.cursor()
        olap_cur = olap_conn.cursor()
        
        # Usar autocommit en OLTP para evitar transacciones abortadas
        oltp_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        if table is None:
            # Modo completo
            print('Sincronizando clientes...')
            _sync_clientes(oltp_cur, olap_cur)
            print('Sincronizando categorias...')
            _sync_categorias(oltp_cur, olap_cur)
            print('Sincronizando productos...')
            _sync_productos(oltp_cur, olap_cur)
            print('Sincronizando hechos de ventas...')
            _sync_ventas(oltp_cur, olap_cur)
        else:
            # Modo incremental por tabla/registro
            table = table.lower()
            print(f"Sincronización incremental | Tabla: {table} | Operación: {operation} | ID: {record_id}")
            if table == 'clientes':
                _sync_clientes(oltp_cur, olap_cur, record_id)
            elif table == 'categoria':
                _sync_categorias(oltp_cur, olap_cur, record_id)
            elif table == 'productos':
                _sync_productos(oltp_cur, olap_cur, record_id)
            elif table == 'ventas':
                _sync_ventas(oltp_cur, olap_cur, id_venta=record_id)
            elif table == 'orden':
                # Reprocesa hechos por id_orden
                _sync_ventas(oltp_cur, olap_cur, id_orden=record_id)
                # Actualiza dimensión cliente relacionada (por si cambió dir. envío)
                oltp_cur.execute('SELECT id_cliente FROM orden WHERE id_orden = %s;', (record_id,))
                row = oltp_cur.fetchone()
                if row:
                    _sync_clientes(oltp_cur, olap_cur, row['id_cliente'])
            elif table == 'orden_producto':
                # Obtiene id_orden a partir de la línea - usando diferentes posibles nombres de PK
                row = None
                for pk_field in ['id_op', 'id_orden_producto', 'id']:
                    try:
                        oltp_cur.execute(f'SELECT id_orden FROM orden_producto WHERE {pk_field} = %s;', (record_id,))
                        row = oltp_cur.fetchone()
                        if row:
                            break
                    except Exception as e:
                        # Si falla una consulta, continúa con el siguiente campo
                        olap_conn.rollback()  # Reset de transacción
                        continue
                if row:
                    _sync_ventas(oltp_cur, olap_cur, id_orden=row['id_orden'])
            else:
                # Si no reconocemos la tabla, hacemos full sync por seguridad
                sync_all(oltp_cur, olap_cur)

        olap_conn.commit()
        print("Sincronización OLTP → OLAP completada con éxito.")
    except Exception as e:
        olap_conn.rollback()
        print(f"Error en la sincronización: {e}")
        traceback.print_exc() 
    finally:
        oltp_conn.close()
        olap_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sincronización OLTP → OLAP (full o incremental).')
    parser.add_argument('--table', type=str, default=None, help='Tabla afectada (clientes, categoria, productos, orden, orden_producto, ventas)')
    parser.add_argument('--op', type=str, default=None, help='Operación (insert, update, delete)')
    parser.add_argument('--id', type=int, default=None, help='ID del registro afectado')
    args = parser.parse_args()

    sync_oltp_to_olap(table=args.table, operation=args.op, record_id=args.id)