# 🚀 Quick Start Guide - Proyecto9 Grupo4

**Para miembros del equipo que reciben el repositorio por primera vez.**

## 📋 Requisitos

- ✅ **Docker Desktop** instalado y corriendo
- ✅ **Git** instalado
- ✅ Acceso a **MongoDB Atlas** (credenciales)

## ⚡ Inicio Rápido (5 minutos)

### 1️⃣ Clonar repositorio

```bash
git clone https://github.com/Bootcamp-IA-P5/proyecto9-grupo4.git
cd proyecto9-grupo4
```

### 2️⃣ Configurar credenciales

```bash
cp .env.example .env
```

Edita `.env` y añade:
```bash
MONGO_ATLAS_URI=mongodb+srv://usuario:contraseña@cluster.mongodb.net/
```

### 3️⃣ Levantar Airflow

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

### 4️⃣ Obtener credenciales de acceso

```bash
docker logs airflow-webserver 2>&1 | grep "Password for user"
```

Verás algo como:
```
Simple auth manager | Password for user 'admin': ABC123XYZ789
```

### 5️⃣ Acceder a Airflow

- **URL:** http://localhost:8080
- **Usuario:** `admin`
- **Password:** El que obtuviste en el paso 4

---

## 🎯 Qué hacer después

### Activar el DAG de monitoreo

1. En la UI de Airflow, busca: **`kafka_mongodb_health_monitor`**
2. Activa el toggle (se pone azul/verde)
3. Se ejecutará automáticamente cada 10 minutos

### Ejecutar el consumer de Kafka

En otra terminal:
```bash
python scripts/read_from_kafka.py
```

### Ver resultados

En el DAG, click en la tarea `generate_health_summary` → "Log"

---

## 🛑 Comandos útiles

**Ver logs en tiempo real:**
```bash
docker logs -f airflow-webserver
```

**Detener Airflow (mantiene contraseña):**
```bash
docker-compose -f docker-compose-airflow.yml down
```

**Detener y limpiar todo (regenera contraseña):**
```bash
docker-compose -f docker-compose-airflow.yml down -v
```

**Ver estado de contenedores:**
```bash
docker-compose -f docker-compose-airflow.yml ps
```

---

## 📚 Documentación completa

- **Airflow detalles:** [`airflow/README.md`](airflow/README.md)
- **Contribuir al proyecto:** [`CONTRIBUTING.md`](CONTRIBUTING.md)
- **Arquitectura del proyecto:** [`README.md`](README.md)

---

## 🆘 Problemas comunes

### ❌ "Docker is not running"
→ Abre Docker Desktop y espera a que cargue completamente

### ❌ "Cannot connect to MongoDB"
→ Verifica que `MONGO_ATLAS_URI` en `.env` es correcto y que la IP está en whitelist

### ❌ "El DAG no aparece"
→ Espera 30 segundos (Airflow escanea cada 30 segundos)

### ❌ "Tasa de inserción = 0"
→ El consumer de Kafka no está corriendo. Ejecuta: `python scripts/read_from_kafka.py`

---

**¿Problemas?** Contacta con el equipo de data engineering en el Slack del bootcamp 📧
