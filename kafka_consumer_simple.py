#!/usr/bin/env python3
"""
Consumidor Kafka simplificado - Lee y guarda 10000 mensajes
Sin análisis de offsets complejo, enfocado en lectura pura
"""
import os
import time
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:29092")
TOPIC = os.getenv("TOPIC", "probando")
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "10000"))


def main():
    print("\n🚀 CONSUMIDOR KAFKA SIMPLIFICADO")
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
            enable_auto_commit=False,  # Deshabilitar auto-commit para control manual
            group_id=None,  # Sin grupo para evitar problemas de offset
            consumer_timeout_ms=5000,  # Timeout si no hay mensajes
            max_poll_records=500  # Leer más mensajes por poll
        )
        print("✅ Conectado exitosamente")

        # Verificar particiones disponibles
        partitions = consumer.partitions_for_topic(TOPIC)
        print(f"📊 Particiones disponibles: {partitions}")

        buffer = []
        message_count = 0
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
                    if elapsed % 5 == 0:
                        print(
                            f"⏳ Esperando mensajes... ({elapsed}s, {len(buffer)}/{BUFFER_SIZE})")
                    continue

                # Procesar cada partición y sus mensajes
                for topic_partition, records in messages.items():
                    for msg in records:
                        message_count += 1

                        try:
                            # Decodificar valor
                            value = msg.value
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')

                            # Intentar parsear como JSON
                            try:
                                data = json.loads(value)
                            except json.JSONDecodeError:
                                data = value

                            # Agregar al buffer
                            buffer.append(data)

                            # Mostrar progreso
                            progress = (len(buffer) * 100) // BUFFER_SIZE
                            if progress > last_progress:
                                elapsed = int(time.time() - start_time)
                                rate = len(buffer) / \
                                    elapsed if elapsed > 0 else 0
                                print(f"✓ Progreso: {progress}% ({len(buffer)}/{BUFFER_SIZE}) | "
                                      f"Velocidad: {rate:.1f} msg/s | Tiempo: {elapsed}s")
                                last_progress = progress

                            # Si alcanzamos el objetivo, salir
                            if len(buffer) >= BUFFER_SIZE:
                                break

                        except Exception as e:
                            print(
                                f"\n⚠️  Error procesando mensaje #{message_count}: {e}")

                    if len(buffer) >= BUFFER_SIZE:
                        break

                # Hacer commit de offsets
                consumer.commit()

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"\n⚠️  Error en poll: {e}")
                continue

        # Buffer completo - guardar
        elapsed = time.time() - start_time
        save_buffer(buffer, elapsed)

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


def save_buffer(buffer, elapsed, reason="completado"):
    """Guardar buffer en archivo JSON"""
    timestamp = int(time.time())
    filename = f"kafka_messages_{timestamp}.json"

    try:
        with open(filename, 'w') as f:
            json.dump({
                "metadata": {
                    "total_messages": len(buffer),
                    "duration_seconds": round(elapsed, 2),
                    "messages_per_second": round(len(buffer) / elapsed, 2) if elapsed > 0 else 0,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "topic": TOPIC,
                    "broker": KAFKA_BOOTSTRAP_SERVERS,
                    "reason": reason
                },
                "messages": buffer
            }, f, indent=2)

        size_kb = os.path.getsize(filename) / 1024
        print(f"\n✅ Guardado exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"   - Mensajes: {len(buffer)}")
        print(f"   - Duración: {elapsed:.2f}s")
        print(f"   - Velocidad: {len(buffer)/elapsed:.2f} msg/s")
        print(f"   - Archivo: {os.path.abspath(filename)}")
        print(f"   - Tamaño: {size_kb:.2f} KB")

    except Exception as e:
        print(f"❌ Error guardando archivo: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
