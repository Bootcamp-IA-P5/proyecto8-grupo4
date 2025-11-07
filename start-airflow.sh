#!/bin/bash
# Script to start Airflow in Docker (Windows compatible)

echo "🚀 Starting Apache Airflow with Docker..."
echo ""

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running."
    echo "   Please start Docker Desktop and try again."
    exit 1
fi

# Start services
echo "📦 Starting Airflow services..."
docker-compose -f docker-compose-airflow.yml up -d

echo ""
echo "✅ Airflow started successfully!"
echo ""
echo "📊 Access Airflow UI:"
echo "   URL: http://localhost:8080"
echo "   Credentials: Check logs with 'docker logs airflow-webserver | grep Password'"
echo ""
echo "📝 View logs:"
echo "   Webserver: docker logs -f airflow-webserver"
echo "   Scheduler: docker logs -f airflow-scheduler"
echo ""
echo "🛑 To stop Airflow:"
echo "   docker-compose -f docker-compose-airflow.yml down"
echo ""
