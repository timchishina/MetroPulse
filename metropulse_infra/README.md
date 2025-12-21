# MetroPulse Infrastructure & Data Generation

Эта директория содержит Docker-инфраструктуру и ELT-скрипты генерации данных для проекта MetroPulse.

## Сервисы и порты

| Сервис | Хост Порт | Внутр. Порт | Логин / Пароль | Комментарий |
|--------|-----------|-------------|----------------|-------------|
| **MinIO API (S3)** | **`9005`** | 9000 | `minioadmin`/`minioadmin` | **Сюда стучаться кодом (Spark)!** |
| MinIO Console | `9001` | 9001 | `minioadmin`/`minioadmin` | Web UI для просмотра файлов |
| **ClickHouse (Native)**| **`9002`** | 9000 | (default) | **Для JDBC драйверов** |
| ClickHouse (HTTP) | `8123` | 8123 | (default) | Для DBeaver / Web |
| PostgreSQL | `5432` | 5432 | `user`/`password` | DB: `metropulse_dwh` |
| Kafka Broker | `9092` | 9092 | (no auth) | External listener |
| Spark Master | `8080` | 8080 | - | Web UI |

## Quick Start

### 1. Запуск инфраструктуры
```bash
docker-compose up -d
```

### 2. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 3. Генерация исторических данных (Batch)
```bash
python3 scripts/gen_batch.py
```
Генерирует Users, Routes, Vehicles, Rides и Payments в MinIO (бакет `raw-data`).

### 4. Запуск потока событий (Stream)
```bash
python3 scripts/gen_stream.py
```
Эмулирует движение транспорта в реальном времени в Kafka (топик `vehicle_positions`).

