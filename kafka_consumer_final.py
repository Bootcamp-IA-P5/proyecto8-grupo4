#!/usr/bin/env python3
"""
Kafka Consumer FINAL - Híbrido que SÍ FUNCIONA
Usa kafka-python (que conecta exitosamente) + models.py para clasificar
"""
import os
import time
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from models import (
    PersonalData, Location, ProfessionalData,
    BankData, NetData, identify_message_type
)

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:29092")
TOPIC = os.getenv("TOPIC", "probando")
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "10000"))
OUTPUT_FILE = f"kafka_messages_final_{int(time.time())}.json"

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
    """
    message_type = identify_message_type(data)

    if message_type in stats:
        stats[message_type] += 1

    # Mostrar cada 100 mensajes de cada tipo
    if stats[message_type] % 100 == 0:
        if message_type == 'personal':
            try:
                personal = PersonalData.from_dict(data)
                print(f"   📋 PERSONAL: {personal.name} {personal.last_name}")
            except:
                pass
        elif message_type == 'location':
            try:
                location = Location.from_dict(data)
                print(f"   📍 LOCATION: {location.city}")
            except:
                pass
        elif message_type == 'professional':
            try:
                prof = ProfessionalData.from_dict(data)
                print(f"   💼 PROFESSIONAL: {prof.company}")
            except:
                pass
        elif message_type == 'bank':
            try:
                bank = BankData.from_dict(data)
                print(f"   💰 BANK: IBAN {bank.IBAN[:10]}...")
            except:
                pass
        elif message_type == 'net':
            try:
                net = NetData.from_dict(data)
                print(f"   🌐 NET: {net.IPv4}")
            except:
                pass

    return message_type


def save_buffer(buffer, elapsed, reason="completado"):
    """Guardar buffer en archivo JSON con estadísticas"""
    if not buffer:
        print("⚠️ No hay mensajes para guardar")
        return

    try:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump({
                "metadata": {
                    "total_messages": len(buffer),
                    "duration_seconds": round(elapsed, 2),
                    "messages_per_second": round(len(buffer) / elapsed, 2) if elapsed > 0 else 0,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "topic": TOPIC,
                    "broker": KAFKA_BOOTSTRAP_SERVERS,
                    "reason": reason,
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
        print("\n" + "=" * 80)
        print(f"✅ Archivo guardado: {OUTPUT_FILE}")
        print(
            f"📊 {len(buffer)} mensajes | {elapsed:.2f}s | {len(buffer)/elapsed:.1f} msg/s")
        print(f"📦 Tamaño: {size_kb:.2f} KB")
        print(f"\n📋 Tipos de mensajes:")
        print(f"   - 📋 Personal: {stats['personal']}")
        print(f"   - 📍 Location: {stats['location']}")
        print(f"   - 💼 Professional: {stats['professional']}")
        print(f"   - 💰 Bank: {stats['bank']}")
        print(f"   - 🌐 Net: {stats['net']}")
        print(f"   - ❓ Unknown: {stats['unknown']}")
        print(f"   - ❌ Errors: {stats['errors']}")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error guardando archivo: {e}")


def main():
    print("\n🚀 KAFKA CONSUMER FINAL - kafka-python + models.py")
    print("=" * 80)
    print(f"📍 Servidor: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📑 Topic: {TOPIC}")
    print(f"📦 Objetivo: {BUFFER_SIZE} mensajes")
    print("=" * 80)

    try:
        print("\n🔌 Conectando a Kafka...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',  # Leer desde el inicio
            enable_auto_commit=False,  # Control manual
            group_id=None,  # Sin grupo = lectura simple como el consumer que funciona
            consumer_timeout_ms=5000,
            max_poll_records=500
        )
        print("✅ Conectado exitosamente")

        # Verificar particiones disponibles
        partitions = consumer.partitions_for_topic(TOPIC)
        print(f"📊 Particiones disponibles: {partitions}")

        buffer = []
        start_time = time.time()
        last_progress = 0

        print(f"\n📥 Iniciando lectura de mensajes...")
        print(f"Presiona Ctrl+C para interrumpir\n")

        while len(buffer) < BUFFER_SIZE:
            try:
                # Leer un lote de mensajes
                messages = consumer.poll(timeout_ms=1000, max_records=100)

                if not messages:
                    elapsed = int(time.time() - start_time)
                    if elapsed % 10 == 0 and elapsed > 0:
                        print(
                            f"⏳ Esperando mensajes... ({elapsed}s, {len(buffer)}/{BUFFER_SIZE})")
                    continue

                # Procesar cada partición y sus mensajes
                for topic_partition, records in messages.items():
                    for msg in records:
                        try:
                            # Decodificar valor
                            value = msg.value
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')

                            # Parsear JSON
                            try:
                                data = json.loads(value)
                            except json.JSONDecodeError:
                                data = {"raw": value}
                                stats['unknown'] += 1
                                continue

                            # Identificar tipo de mensaje
                            message_type = process_message(data)

                            # Agregar al buffer con metadata
                            buffer.append({
                                "offset": msg.offset,
                                "partition": msg.partition,
                                "timestamp": msg.timestamp,
                                "type": message_type,
                                "data": data
                            })

                            stats['total_messages'] += 1

                            # Mostrar progreso
                            progress = (len(buffer) * 100) // BUFFER_SIZE
                            if progress > last_progress and progress % 5 == 0:
                                elapsed = int(time.time() - start_time)
                                rate = len(buffer) / \
                                    elapsed if elapsed > 0 else 0
                                print(f"✓ Progreso: {progress}% ({len(buffer)}/{BUFFER_SIZE}) | "
                                      f"Velocidad: {rate:.1f} msg/s")
                                last_progress = progress

                            # Si alcanzamos el objetivo, salir
                            if len(buffer) >= BUFFER_SIZE:
                                break

                        except Exception as e:
                            stats['errors'] += 1
                            print(f"\n⚠️  Error procesando mensaje: {e}")

                    if len(buffer) >= BUFFER_SIZE:
                        break

                # Hacer commit de offsets cada 100 mensajes
                if len(buffer) % 100 == 0:
                    consumer.commit()

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"\n⚠️  Error en poll: {e}")
                continue

        # Buffer completo - guardar
        elapsed = time.time() - start_time
        save_buffer(buffer, elapsed)

        print(f"\n✨ ¡Completado! {len(buffer)} mensajes capturados")
        print(f"📁 Archivo: {os.path.abspath(OUTPUT_FILE)}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrumpido por usuario")
        if buffer:
            elapsed = time.time() - start_time
            print(f"\n💾 Guardando {len(buffer)} mensajes recolectados...")
            save_buffer(buffer, elapsed, reason="interrumpido")
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            consumer.close()
            print("✓ Conexión cerrada")
        except:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
