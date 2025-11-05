[Gestión del Proyecto](https://github.com/orgs/Bootcamp-IA-P5/projects/16)

# Project structure

```
nombre_proyecto/
├── src/
│   ├── core/
│   │   ├── kafka_consumer.py        # 1. Leer de Kafka y escribir en MongoDB (Colección A)
│   │   ├── data_processor.py      # 2. Lógica de procesamiento: A -> B
│   │   └── rel_writer.py          # 3. Leer de B y escribir en Relacional
│   ├── database/
│   │   ├── mongodb_connector.py     # Clases/funciones para interactuar con MongoDB
│   │   └── sql_connector.py         # Clases/funciones para interactuar con la DB Relacional
│   └── __init__.py                # Hace que 'src' sea un paquete Python
├── config/
│   ├── settings.py                # Variables de entorno/configuración leídas al inicio
│   └── kafka.ini                  # Archivo de configuración específico de Kafka (o .env)
├── tests/
│   ├── test_kafka_consumer.py
│   ├── test_data_processor.py
│   └── test_mongodb_connector.py
├── scripts/
│   └── run_processor.py           # Script para iniciar el proceso de forma manual o programada
├── requirements.txt               # Lista de dependencias del proyecto (p. ej., kafka-python, pymongo, sqlalchemy)
└── README.md                      # Documentación del proyecto
```

