"""
Observation DAG - Monitors Kafka → MongoDB pipeline health
Does NOT execute the consumer, only observes the system state.

This DAG monitors the pipeline without modifying anything.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv('MONGO_ATLAS_URI', 'mongodb://localhost:27017')
MONGO_DATABASE = 'kafka_data'
MONGO_COLLECTION = 'probando_messages'

# DAG configuration
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='kafka_mongodb_health_monitor',
    default_args=default_args,
    description='Monitorea salud del pipeline Kafka→MongoDB (solo observación)',
    schedule='*/10 * * * *',  # Every 10 minutes
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['observation', 'kafka', 'mongodb', 'health'],
)


# ============================================================================
# TASK 1: Verify MongoDB Connection
# ============================================================================
def check_mongodb_connection(**context):
    """
    Verifies that MongoDB is available and accessible.
    This is the most basic task - just attempts to connect.
    """
    logging.info("🔍 Verificando conexión a MongoDB...")
    
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000
        )
        
        # Ping to verify real connection
        client.admin.command('ping')
        logging.info("✓ MongoDB está accesible")
        
        # List available databases
        databases = client.list_database_names()
        logging.info(f"✓ Bases de datos disponibles: {databases}")
        
        # Verify that our database exists
        if MONGO_DATABASE in databases:
            logging.info(f"✓ Base de datos '{MONGO_DATABASE}' existe")
        else:
            logging.warning(f"⚠ Base de datos '{MONGO_DATABASE}' no existe aún")
        
        client.close()
        return True
        
    except Exception as e:
        logging.error(f"✗ MongoDB no accesible: {type(e).__name__}: {e}")
        raise


# ============================================================================
# TASK 2: Verify Data Freshness
# ============================================================================
def check_data_freshness(**context):
    """
    Verifies that data in MongoDB is recent.
    This tells you if your read_from_kafka.py script is working.
    """
    logging.info("🔍 Verificando frescura de datos...")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        # Count total documents
        total_docs = collection.count_documents({})
        logging.info(f"📊 Total de documentos en colección: {total_docs}")
        
        if total_docs == 0:
            logging.warning("⚠ La colección está vacía - no se han ingestado datos aún")
            client.close()
            return {
                'status': 'empty',
                'total_documents': 0,
                'message': 'No hay datos aún'
            }
        
        # Get last inserted document (using _id which contains timestamp)
        latest_doc = collection.find_one(sort=[('_id', -1)])
        
        if latest_doc:
            # Extract timestamp from MongoDB ObjectId
            doc_timestamp = latest_doc['_id'].generation_time.replace(tzinfo=None)
            now = datetime.now()
            age = now - doc_timestamp
            
            logging.info(f"📅 Último documento insertado hace: {age}")
            logging.info(f"📅 Timestamp: {doc_timestamp.isoformat()}")
            
            # Define freshness threshold (15 minutes)
            threshold = timedelta(minutes=15)
            
            if age > threshold:
                logging.warning(f"⚠ Los datos están desactualizados!")
                logging.warning(f"   Última inserción: {age} (threshold: {threshold})")
                status = 'stale'
            else:
                logging.info(f"✓ Datos frescos: última inserción hace {age}")
                status = 'fresh'
            
            result = {
                'status': status,
                'total_documents': total_docs,
                'last_insert_age_seconds': age.total_seconds(),
                'last_insert_time': doc_timestamp.isoformat(),
                'age_human_readable': str(age),
            }
            
            # Save to XCom so other tasks can use this data
            context['task_instance'].xcom_push(key='freshness_check', value=result)
            
            client.close()
            return result
            
        else:
            logging.warning("⚠ No se pudo obtener el último documento")
            client.close()
            return {'status': 'unknown', 'total_documents': total_docs}
            
    except Exception as e:
        logging.error(f"✗ Error verificando frescura: {type(e).__name__}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise


# ============================================================================
# TASK 3: Calculate Insertion Rate
# ============================================================================
def calculate_insertion_rate(**context):
    """
    Calculates how many documents are being inserted per minute.
    Helps you understand your pipeline's throughput.
    """
    logging.info("🔍 Calculando tasa de inserción...")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        # Documents from the last 10 minutes
        ten_min_ago = datetime.now() - timedelta(minutes=10)
        
        # Count recent documents (MongoDB's _id contains timestamp)
        # Note: This works because ObjectId has embedded timestamp
        from bson import ObjectId
        
        # Create ObjectId with timestamp from 10 minutes ago
        cutoff_id = ObjectId.from_datetime(ten_min_ago)
        
        recent_docs = collection.count_documents({
            '_id': {'$gte': cutoff_id}
        })
        
        rate_per_minute = recent_docs / 10.0
        
        logging.info(f"📈 Documentos últimos 10 min: {recent_docs}")
        logging.info(f"📈 Tasa de inserción: {rate_per_minute:.2f} docs/min")
        
        # Additional analysis
        if rate_per_minute == 0:
            logging.warning("⚠ No hay inserciones recientes - el consumidor puede estar detenido")
        elif rate_per_minute < 1:
            logging.warning(f"⚠ Tasa de inserción baja: {rate_per_minute:.2f} docs/min")
        else:
            logging.info(f"✓ Pipeline activo con {rate_per_minute:.2f} docs/min")
        
        result = {
            'recent_documents': recent_docs,
            'rate_per_minute': rate_per_minute,
            'period_minutes': 10,
            'estimated_daily_rate': rate_per_minute * 60 * 24
        }
        
        context['task_instance'].xcom_push(key='insertion_rate', value=result)
        
        client.close()
        return result
        
    except Exception as e:
        logging.error(f"✗ Error calculando tasa: {type(e).__name__}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise


# ============================================================================
# TASK 4: Generate Health Summary
# ============================================================================
def generate_health_summary(**context):
    """
    Generates a visual summary of the complete pipeline status.
    This task combines the results of all previous tasks.
    """
    logging.info("📋 Generando resumen de salud del pipeline...")
    
    ti = context['task_instance']
    
    # Get results from previous tasks using XCom
    freshness = ti.xcom_pull(task_ids='check_data_freshness', key='freshness_check')
    rate = ti.xcom_pull(task_ids='calculate_insertion_rate', key='insertion_rate')
    
    # Determine overall status
    if not freshness or freshness.get('status') == 'empty':
        pipeline_status = 'no_data'
    elif freshness.get('status') == 'stale':
        pipeline_status = 'degraded'
    elif rate and rate.get('rate_per_minute', 0) == 0:
        pipeline_status = 'stalled'
    else:
        pipeline_status = 'healthy'
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'pipeline_status': pipeline_status,
        'data_freshness': freshness,
        'insertion_metrics': rate
    }
    
    # Formatted and beautiful log
    logging.info("=" * 70)
    logging.info("📊 RESUMEN DE SALUD DEL PIPELINE KAFKA → MongoDB")
    logging.info("=" * 70)
    logging.info(f"🚦 Estado General: {pipeline_status.upper()}")
    logging.info("-" * 70)
    
    if freshness:
        logging.info(f"📦 Total documentos: {freshness.get('total_documents', 'N/A')}")
        logging.info(f"🕐 Última inserción: {freshness.get('age_human_readable', 'N/A')}")
        logging.info(f"✨ Estado datos: {freshness.get('status', 'N/A').upper()}")
    
    if rate:
        logging.info(f"📈 Tasa inserción: {rate.get('rate_per_minute', 0):.2f} docs/min")
        logging.info(f"📊 Últimos 10 min: {rate.get('recent_documents', 0)} documentos")
        logging.info(f"📅 Estimación diaria: {rate.get('estimated_daily_rate', 0):.0f} docs/día")
    
    logging.info("=" * 70)
    
    # Recommendations based on status
    logging.info("\n💡 RECOMENDACIONES:")
    if pipeline_status == 'no_data':
        logging.info("   → Ejecuta scripts/read_from_kafka.py para comenzar la ingesta")
    elif pipeline_status == 'degraded':
        logging.info("   → Los datos están desactualizados. Verifica que read_from_kafka.py esté corriendo")
    elif pipeline_status == 'stalled':
        logging.info("   → No hay inserciones recientes. Verifica Kafka y el consumidor")
    else:
        logging.info("   → Todo funcionando correctamente ✓")
    
    logging.info("")
    
    return summary


# ============================================================================
# DEFINE DAG TASKS
# ============================================================================

task_check_mongo = PythonOperator(
    task_id='check_mongodb_connection',
    python_callable=check_mongodb_connection,
    dag=dag,
)

task_check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

task_calculate_rate = PythonOperator(
    task_id='calculate_insertion_rate',
    python_callable=calculate_insertion_rate,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='generate_health_summary',
    python_callable=generate_health_summary,
    dag=dag,
)


# ============================================================================
# DEFINE EXECUTION ORDER (DEPENDENCIES)
# ============================================================================
# Tasks execute in this order:
# 1. Verify MongoDB connection
# 2. Verify data freshness
# 3. Calculate insertion rate
# 4. Generate final summary

task_check_mongo >> task_check_freshness >> task_calculate_rate >> task_summary
