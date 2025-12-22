# Задача 3 — Batch ETL в DWH (Spark)

## Что делает моя часть
Скрипт `etl_batch.py` выполняет Batch ETL:
1) читает сырые Parquet-файлы из MinIO (S3),
2) очищает/приводит типы,
3) загружает измерения и факты в DWH (PostgreSQL),
4) реализует SCD Type 2 для измерения маршрутов (например, `dim_route`),
5) обеспечивает повторный запуск (идемпотентность на уровне батча).

---

## Предусловия
Перед запуском ETL должны быть выполнены условия:
1) Инфраструктура поднята (docker compose up -d).
2) Сырые данные сгенерированы и лежат в MinIO (это делает `scripts/gen_batch.py`).
3) В Postgres созданы таблицы DWH (DDL из задачи 1 уже применён).

Если пункты (2) и (3) не выполнены, ETL упадёт: нечего читать или некуда писать.

---

## Установка Python-зависимостей (только для запуска ETL)
Все зависимости описаны в `requirements.txt`.

Рекомендуемый способ (venv):
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

---

## Важное про MinIO endpoint
В проекте принято:
- MinIO доступен по адресу localhost:9005
- Postgres доступен на 5432

Но localhost зависит от того, где запущен Spark:
- Если запускаешь Spark на хосте:
  - MinIO: http://localhost:9005
  - Postgres: localhost:5432
- Если запускаешь Spark в docker-контейнере:
  - localhost:9005 будет указывать на сам контейнер, поэтому MinIO не найдётся.
  - Тогда используй один из вариантов (зависит от вашей сети/compose):
    - http://minio:9000 (доступ по имени сервиса в docker-сети)
    - или http://host.docker.internal:9005 (доступ к хостовому порту из контейнера)

---

## Переменные окружения для ETL
Скрипт `etl_batch.py` читает настройки из env.

MinIO (S3):
- S3_ENDPOINT — endpoint MinIO (например, http://localhost:9005)
- S3_ACCESS_KEY — ключ доступа
- S3_SECRET_KEY — секрет
- S3_BUCKET — bucket с raw parquet

Postgres (DWH):
- PG_HOST — хост Postgres
- PG_PORT — порт (обычно 5432)
- PG_DB — база (например, metropulse_dwh)
- PG_USER — пользователь
- PG_PASSWORD — пароль

Пример для запуска ETL на хосте:
export S3_ENDPOINT="http://localhost:9005"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="minioadmin"
export S3_BUCKET="raw-data"

export PG_HOST="localhost"
export PG_PORT="5432"
export PG_DB="metropulse_dwh"
export PG_USER="user"
export PG_PASSWORD="password"

---

## Запуск ETL

### Вариант A: запуск Spark на хосте (если Spark установлен локально)
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
  etl_batch.py --run_ts "2025-12-22T12:00:00"

### Вариант B: запуск spark-submit внутри контейнера (если Spark в docker)
Зайти в контейнер spark-master:
docker exec -it mp_spark_master bash

Выставить env (пример — для доступа к MinIO через docker-сеть):
export S3_ENDPOINT="http://minio:9000"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="minioadmin"
export S3_BUCKET="raw-data"

export PG_HOST="postgres"
export PG_PORT="5432"
export PG_DB="metropulse_dwh"
export PG_USER="user"
export PG_PASSWORD="password"

Запуск:
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
  etl_batch.py --run_ts "2025-12-22T12:00:00"

---

## Что загружает ETL (результат)
После выполнения ETL в Postgres должны быть заполнены:
- dim_user (обновляется батчем)
- dim_vehicle (обновляется батчем)
- dim_datetime (пополняется новыми timestamp)
- dim_route (SCD Type 2)
- fact_ride
- fact_payment

---

## Проверки после загрузки (smoke tests)
Подключиться к Postgres и проверить:
SELECT COUNT(*) FROM dim_user;
SELECT COUNT(*) FROM dim_vehicle;
SELECT COUNT(*) FROM dim_route;
SELECT COUNT(*) FROM dim_datetime;
SELECT COUNT(*) FROM fact_ride;
SELECT COUNT(*) FROM fact_payment;

Проверка SCD2 (пример для одного route_id):
SELECT route_id_nk, base_fare, valid_from, valid_to, is_current
FROM dim_route
WHERE route_id_nk = 1
ORDER BY valid_from;

---

## Как показать SCD Type 2 на защите
1) Запустить ETL (первый прогон).
2) Изменить атрибут маршрута в raw данных (например, base_fare у route_id=1) и заново положить routes.parquet в MinIO.
3) Запустить ETL второй раз с более поздним --run_ts.
4) Выполнить SQL-проверку SCD2 и показать две версии записи: старая закрыта, новая актуальна.

---

## Идемпотентность (повторный запуск)
- Повторный запуск ETL с теми же входными данными не должен ломать схему и давать мусорные дубликаты.
- SCD2 обновляет dim_route только если изменились сравниваемые атрибуты; если изменений нет — новых версий не создаётся.
- Остальные таблицы могут обновляться батчем (в учебном прототипе допускается full refresh для фактов/части измерений).
