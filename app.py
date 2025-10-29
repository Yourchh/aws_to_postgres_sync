import os
import boto3
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from flask import Flask, jsonify
from dotenv import load_dotenv
from flask_cors import CORS, cross_origin # Importar cross_origin
import atexit # Para apagar el scheduler
from apscheduler.schedulers.background import BackgroundScheduler # Para tareas en segundo plano

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# --- Configuración de la Aplicación Flask ---
app = Flask(__name__)
# Configurar CORS para permitir peticiones desde http://localhost:3000 (tu React app)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

# --- Configuración de Boto3 para DynamoDB ---
DYNAMO_TABLE_NAME = 'datos_sensores' # ¡Usa el nombre exacto de tu tabla!
dynamodb = boto3.resource('dynamodb', region_name='us-east-1') # Cambia a tu región
dynamo_table = dynamodb.Table(DYNAMO_TABLE_NAME)

# --- Configuración de la Conexión a PostgreSQL ---
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    """Crea y retorna una nueva conexión a la base de datos."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

# ====================================================================
# FUNCIÓN DE SINCRONIZACIÓN (MODIFICADA CON MÁS LOGS)
# ====================================================================
def run_dynamo_sync():
    """
    Esta función contiene toda la lógica para sincronizar DynamoDB con PostgreSQL.
    Ahora es llamada por el scheduler.
    """
    print("--- SCHEDULER: Intentando ejecutar run_dynamo_sync ---") # <-- NUEVO LOG
    # Necesitamos el contexto de la app para que la función se ejecute correctamente
    with app.app_context():
        print("--- SCHEDULER: App Context cargado. Iniciando Sincronización. ---") # <-- LOG MEJORADO
        
        # 1. Escanear y obtener todos los datos de DynamoDB
        try:
            items = []
            scan_kwargs = {}
            while True:
                response = dynamo_table.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                if 'LastEvaluatedKey' not in response:
                    break
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
            if not items:
                print("Sync: No se encontraron nuevos items en DynamoDB.")
                return {"status": "no_items"}
            
            print(f"Sync: Se encontraron {len(items)} items en DynamoDB.")
        
        except Exception as e:
            print(f"Sync: Error al escanear DynamoDB: {e}")
            return {"status": "error_dynamo", "error": str(e)}

        # 2. Preparar los datos
        datos_para_insertar = []
        for item in items:
            datos_para_insertar.append((
                item.get('id_lectura'), item.get('device_id'),
                float(item.get('temperatura', 0.0)), float(item.get('humedad', 0.0)),
                float(item.get('distancia_cm', 0.0)), int(item.get('luz_porcentaje', 0)),
                item.get('estado_luz'), int(item.get('timestamp'))
            ))

        # 3. Insertar los datos en PostgreSQL
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 3a. Crear una tabla temporal
            temp_table_query = """
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_lecturas (
                id_lectura VARCHAR(255) PRIMARY KEY,
                device_id VARCHAR(50),
                temperatura NUMERIC(5, 2),
                humedad NUMERIC(5, 2),
                distancia_cm NUMERIC(10, 3),
                luz_porcentaje INTEGER,
                estado_luz VARCHAR(20),
                timestamp_lectura_unix BIGINT
            ) ON COMMIT DROP;
            """
            cursor.execute(temp_table_query)
            
            # 3b. Insertar masivamente en la tabla temporal
            insert_temp_query = """
                INSERT INTO temp_lecturas (
                    id_lectura, device_id, temperatura, humedad, distancia_cm, 
                    luz_porcentaje, estado_luz, timestamp_lectura_unix
                ) VALUES %s
                ON CONFLICT (id_lectura) DO NOTHING; 
            """
            # Se usa ON CONFLICT DO NOTHING en la tabla temporal por si se repiten datos del scan
            execute_values(cursor, insert_temp_query, datos_para_insertar)
            
            # 3c. Mover datos de la temporal a la permanente, convirtiendo tipos
            insert_final_query = """
                INSERT INTO lecturas_sensores (
                    id_lectura, device_id, temperatura, humedad, distancia_cm, 
                    luz_porcentaje, estado_luz, timestamp_lectura
                )
                SELECT
                    id_lectura::uuid, -- Convertir de VARCHAR a UUID
                    device_id,
                    temperatura,
                    humedad,
                    distancia_cm,
                    luz_porcentaje,
                    estado_luz,
                    to_timestamp(timestamp_lectura_unix) -- Convertir de UNIX int a Timestamp
                FROM temp_lecturas
                ON CONFLICT (id_lectura) DO NOTHING;
            """
            cursor.execute(insert_final_query)
            registros_insertados = cursor.rowcount
            
            conn.commit()
            cursor.close()
            
            print(f"Sync: Sincronización automática completa. Registros nuevos: {registros_insertados}")
            return {"status": "success", "new_records": registros_insertados}

        except Exception as e:
            if conn: conn.rollback()
            print(f"Sync: Error en BD local durante sync: {e}")
            return {"status": "error_postgres", "error": str(e)}
        
        finally:
            if conn: conn.close()

# ====================================================================
# ENDPOINTS DE LA API (MODIFICADO)
# ====================================================================

@app.route('/api/lecturas', methods=['GET'])
@cross_origin() # Asegura que este endpoint pueda ser llamado desde React
def get_lecturas():
    """
    Endpoint principal del Dashboard.
    AHORA SOLO LEE de PostgreSQL. La sincronización corre en segundo plano.
    """
    # --- PASO 1: Ya no se sincroniza aquí ---
    
    # --- PASO 2: Leer y devolver los datos de PostgreSQL ---
    print("Dashboard: Obteniendo datos de PostgreSQL...")
    conn = None
    try:
        conn = get_db_connection()
        # Usamos RealDictCursor para que los resultados sean diccionarios (JSON)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Ordenamos por timestamp para que los gráficos tengan sentido
        cursor.execute("SELECT * FROM lecturas_sensores ORDER BY timestamp_lectura ASC")
        data = cursor.fetchall()
        
        cursor.close()
        return jsonify(data)

    except Exception as e:
        print(f"Dashboard: Error al leer de PostgreSQL: {e}")
        return jsonify({"error": f"Error en la base de datos local: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()


@app.route('/sync-dynamo', methods=['GET'])
def manual_sync():
    """
    Endpoint para forzar una sincronización manual (el que ya tenías).
    Útil para probar.
    """
    result = run_dynamo_sync()
    return jsonify({"message": "Sincronización manual completada.", "result": result})


if __name__ == '__main__':
    # --- Configuración del Tarea Programada (Scheduler) ---
    scheduler = BackgroundScheduler()
    # Ejecuta run_dynamo_sync cada 60 segundos
    scheduler.add_job(func=run_dynamo_sync, trigger="interval", seconds=60)
    scheduler.start()
    print("Sincronización automática iniciada. Se ejecutará cada minuto.")
    
    # Apagar el scheduler cuando la app se cierre
    atexit.register(lambda: scheduler.shutdown())
    
    # --- Disparar la primera sincronización manualmente AHORA ---
    # Esto nos da retroalimentación inmediata sin esperar 1 minuto.
    print("--- Disparando primera sincronización AHORA... ---")
    with app.app_context(): # Se necesita el contexto para esta llamada manual
         run_dynamo_sync()
    print("--- Primera sincronización completada. Iniciando servidor Flask. ---")

    # Inicia la aplicación Flask
    # IMPORTANTE: use_reloader=False evita que el scheduler se inicie dos veces
    app.run(debug=True, port=5001, use_reloader=False)


