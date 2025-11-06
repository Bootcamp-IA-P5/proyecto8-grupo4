#!/usr/bin/env python3
"""
Kafka Consumer con Confluent-Kafka
Lee 10000 mensajes del topic 'probando' y los guarda en un archivo JSON
Usa models.py para identificar y clasificar tipos de mensajes
"""
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging
import os
import time
from dotenv import load_dotenv
from models import (
    PersonalData, Location, ProfessionalData,
    BankData, NetData, identify_message_type
)

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración del consumer
consumer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'host.docker.internal:29092'),
    'group.id': f'buffer-consumer-{int(time.time())}',  # Grupo único cada vez
    'auto.offset.reset': 'earliest',  # Leer desde el principio
    'enable.auto.commit': False,  # Control manual de commits
    'session.timeout.ms': 60000,
    'max.poll.interval.ms': 300000,
    # Configuración crítica para evitar problemas con advertised listeners
    'broker.address.family': 'v4',  # Forzar IPv4
    'socket.timeout.ms': 60000,
    'metadata.max.age.ms': 180000,
    'enable.partition.eof': False,
    # Logging de debug para librdkafka (ver qué está pasando)
    'debug': 'broker,topic,msg',
    'log_level': 0  # 0=emergency, 7=debug
}

TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'probando')
BUFFER_SIZE = int(os.getenv('BUFFER_SIZE', '10000'))
OUTPUT_FILE = f"kafka_messages_confluent_{int(time.time())}.json"

# Estadísticas por tipo de mensaje
stats = {
    'total_messages': 0,
    'personal': 0,
    'location': 0,
    'professional': 0,
    'bank': 0,
    'net': 0,
    'unknown': 0,
    'errors': 0
}


def process_message(data: dict) -> str:
    """
    Identifica el tipo de mensaje y actualiza estadísticas

    Args:
        data: Diccionario con los datos del mensaje

    Returns:
        str: Tipo de mensaje identificado
    """
    message_type = identify_message_type(data)

    if message_type in stats:
        stats[message_type] += 1

    # Mostrar cada 100 mensajes de cada tipo
    if stats[message_type] % 100 == 0:
        if message_type == 'personal':
            personal = PersonalData.from_dict(data)
            logger.info(f"📋 PERSONAL: {personal.name} {personal.last_name}")
        elif message_type == 'location':
            location = Location.from_dict(data)
            logger.info(f"📍 LOCATION: {location.city}")
        elif message_type == 'professional':
            prof = ProfessionalData.from_dict(data)
            logger.info(f"💼 PROFESSIONAL: {prof.company}")
        elif message_type == 'bank':
            bank = BankData.from_dict(data)
            logger.info(f"💰 BANK: IBAN {bank.IBAN[:10]}...")
        elif message_type == 'net':
            net = NetData.from_dict(data)
            logger.info(f"🌐 NET: {net.IPv4}")

    return message_type


def save_buffer(buffer, elapsed):
    """Guardar buffer en archivo JSON con estadísticas"""
    if not buffer:
        logger.warning("⚠️ No hay mensajes para guardar")
        return

    try:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump({
                "metadata": {
                    "total_messages": len(buffer),
                    "duration_seconds": round(elapsed, 2),
                    "messages_per_second": round(len(buffer) / elapsed, 2) if elapsed > 0 else 0,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "topic": TOPIC_NAME,
                    "broker": consumer_config['bootstrap.servers'],
                    "message_types": {
                        "personal": stats['personal'],
                        "location": stats['location'],
                        "professional": stats['professional'],
                        "bank": stats['bank'],
                        "net": stats['net'],
                        "unknown": stats['unknown']
                    }
                },
                "messages": buffer
            }, f, indent=2)

        size_kb = os.path.getsize(OUTPUT_FILE) / 1024
        logger.info("=" * 80)
        logger.info(f"✅ Archivo guardado: {OUTPUT_FILE}")
        logger.info(
            f"📊 {len(buffer)} mensajes | {elapsed:.2f}s | {len(buffer)/elapsed:.1f} msg/s")
        logger.info(f"📦 Tamaño: {size_kb:.2f} KB")
        logger.info(f"\n📋 Tipos de mensajes:")
        logger.info(f"   - 📋 Personal: {stats['personal']}")
        logger.info(f"   - 📍 Location: {stats['location']}")
        logger.info(f"   - 💼 Professional: {stats['professional']}")
        logger.info(f"   - 💰 Bank: {stats['bank']}")
        logger.info(f"   - 🌐 Net: {stats['net']}")
        logger.info(f"   - ❓ Unknown: {stats['unknown']}")
        logger.info(f"   - ❌ Errors: {stats['errors']}")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"❌ Error guardando archivo: {e}")


def consume_messages():
    """Consume mensajes de Kafka hasta llenar el buffer"""
    consumer = Consumer(consumer_config)
    buffer = []

    try:
        # Suscribirse al topic
        consumer.subscribe([TOPIC_NAME])
        logger.info("=" * 80)
        logger.info("🚀 KAFKA CONSUMER - CONFLUENT + MODELS")
        logger.info(f"📡 Broker: {consumer_config['bootstrap.servers']}")
        logger.info(f"📑 Topic: {TOPIC_NAME}")
        logger.info(f"📦 Objetivo: {BUFFER_SIZE} mensajes")
        logger.info("=" * 80)

        start_time = time.time()
        last_progress = 0

        while len(buffer) < BUFFER_SIZE:
            # Poll para obtener mensajes
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    logger.info(
                        f"⏳ Esperando... ({elapsed}s, {len(buffer)}/{BUFFER_SIZE})")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"Fin de partición: {msg.topic()} [{msg.partition()}]")
                else:
                    stats['errors'] += 1
                    logger.error(f"❌ Error: {msg.error()}")
                continue

            try:
                # Decodificar mensaje
                message_value = msg.value().decode('utf-8')

                # Parsear JSON
                try:
                    data = json.loads(message_value)
                except json.JSONDecodeError:
                    data = {"raw": message_value}
                    stats['unknown'] += 1

                # Identificar tipo de mensaje
                message_type = process_message(data)

                # Agregar al buffer
                buffer.append({
                    "offset": msg.offset(),
                    "partition": msg.partition(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] > 0 else None,
                    "type": message_type,
                    "data": data
                })

                stats['total_messages'] += 1

                # Mostrar progreso cada 5%
                progress = (len(buffer) * 100) // BUFFER_SIZE
                if progress > last_progress and progress % 5 == 0:
                    elapsed = int(time.time() - start_time)
                    rate = len(buffer) / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"✓ {progress}% ({len(buffer)}/{BUFFER_SIZE}) | {rate:.1f} msg/s")
                    last_progress = progress

                # Commit cada 100 mensajes
                if len(buffer) % 100 == 0:
                    consumer.commit(asynchronous=False)

                if len(buffer) >= BUFFER_SIZE:
                    break

            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error procesando mensaje: {e}")

        # Guardar buffer
        elapsed = time.time() - start_time
        save_buffer(buffer, elapsed)

        logger.info(f"\n✨ ¡Completado! {len(buffer)} mensajes capturados")
        logger.info(f"📁 Archivo: {os.path.abspath(OUTPUT_FILE)}")

    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrumpido por usuario")
        if buffer:
            elapsed = time.time() - start_time
            save_buffer(buffer, elapsed)
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        logger.info("✅ Consumer cerrado")


if __name__ == "__main__":
    consume_messages()
