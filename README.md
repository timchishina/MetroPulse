# MetroPulse
Аналитическая платформа для сервиса MetroPulse

## 1. Описание источников данных

### 1.1. OLTP PostgreSQL `metropulse_db`

Связи:

- `USERS 1:M RIDES` — пользователь совершает поездки  
- `USERS 1:M PAYMENTS` — пользователь производит платежи  
- `RIDES 1:1 PAYMENTS` — каждая поездка оплачивается одним платежом  
- `RIDES M:1 ROUTES` — поездка по маршруту  
- `ROUTES 1:M VEHICLES` — маршрут обслуживается ТС

Таблицы:

- **USERS**
  - `user_id` (PK, int)
  - `name` (varchar)
  - `email` (varchar, unique)
  - `created_at` (timestamp) — дата регистрации
  - `city` (varchar)

- **ROUTES**
  - `route_id` (PK, int)
  - `route_number` (varchar) — номер маршрута
  - `vehicle_type` (varchar) — 'bus', 'tram', 'metro'
  - `base_fare` (decimal(10,2)) — базовая стоимость

- **VEHICLES**
  - `vehicle_id` (PK, int)
  - `route_id` (FK, int)
  - `license_plate` (varchar)
  - `capacity` (int)

- **RIDES**
  - `ride_id` (PK, uuid)
  - `user_id` (FK → USERS)
  - `route_id` (FK → ROUTES)
  - `vehicle_id` (FK → VEHICLES)
  - `start_time` (timestamp)
  - `end_time` (timestamp)
  - `fare_amount` (decimal(10,2))

- **PAYMENTS**
  - `payment_id` (PK, uuid)
  - `ride_id` (FK → RIDES)
  - `user_id` (FK → USERS)
  - `amount` (decimal(10,2))
  - `payment_method` (varchar: 'card','sbp','wallet')
  - `status` (varchar: 'success','failed','pending')
  - `created_at` (timestamp)

### 1.2. Kafka: топик `vehicle_positions`

```json
{
  "event_id": "uuid",
  "vehicle_id": 101,
  "route_number": "А-72",
  "event_time": "2024-10-27T10:00:10Z",
  "coordinates": {
    "latitude": 55.7558,
    "longitude": 37.6173
  },
  "speed_kmh": 45.5,
  "passengers_estimated": 25
}
```

Ключевые моменты:

- `vehicle_id` связывает поток с таблицей `VEHICLES`.
- `route_number` дублирует маршрут.
- `event_time` — основа для временных окон и агрегаций.
- Эти события мы в batch-режиме будем агрегировать и загружать в факт `fact_vehicle_movement`.

## 2. Выбор методологии моделирования: Kimball vs Data Vault

#### Подход Кимбалла (Dimensional Modeling)

**Суть:**

- Есть **таблицы фактов** (Fact) — события/измеряемые показатели:
  - у нас это `fact_ride`, `fact_payment`, `fact_vehicle_movement`.
- Есть **таблицы измерений** (Dimension) — справочники-«ось анализа»:
  - `dim_user`, `dim_route`, `dim_vehicle`, `dim_datetime`.

**Как выглядел бы MetroPulse по Kimball:**

- **Измерения:**
  - `dim_user` — пользователь (город, имя, email, дата регистрации).
  - `dim_route` — маршрут, с историей тарифов (SCD2).
  - `dim_vehicle` — ТС (номер, вместимость, связка с маршрутом).
  - `dim_datetime` — календарь/время.
- **Факты:**
  - `fact_ride` — поездка:
    - 1 поездка (`RIDES.ride_id`).
  - `fact_payment` — платёж:
    - 1 платёж (`PAYMENTS.payment_id`).
  - `fact_vehicle_movement` — агрегаты по движениям ТС (из Kafka):
    - ТС + маршрут + интервал времени.

**Плюсы для MetroPulse:**

1. **Простая модель для аналитиков и BI:**
   - запросы читаемые: `SELECT ... FROM fact_ride JOIN dim_route JOIN dim_datetime ...`
   - легко объяснить что такое факт/измерение.

2. **Быстрый старт и меньше таблиц:**
   - несколько чётких таблиц, каждый понимает их смысл.
   - меньше «служебной» инфраструктуры.

3. **Хорошо бьётся с задачами проекта:**
   - построение витрин (ClickHouse) — естественное продолжение звезды.
   - SCD2 реализуется просто в `dim_route`.

**Минусы:**

- Если бы источников было много и они сильно отличались, интеграция и управление изменениями схемы были бы сложнее, чем в Data Vault.
- Не даёт «из коробки» формального слоя raw-истории всех изменений атрибутов (это можно сделать, но надо придумывать самим).

#### Подход Data Vault

**Суть:**

- Делим модель на:
  - **Hubs** — бизнес-ключи (User, Route, Vehicle, Ride, Payment, VehiclePositionEvent).
  - **Links** — связи между ключами (User–Ride, Ride–Payment, Route–Vehicle, Vehicle–VehiclePositionEvent и т.д.).
  - **Satellites** — атрибуты и история изменений вокруг хабов/линков.

**Как мог бы выглядеть MetroPulse в Data Vault:**

- **Hubs:**
  - `Hub_User` (user_id),
  - `Hub_Route` (route_id / route_number),
  - `Hub_Vehicle` (vehicle_id),
  - `Hub_Ride` (ride_id),
  - `Hub_Payment` (payment_id),
  - `Hub_VehiclePositionEvent` (event_id из Kafka).

- **Links:**
  - `Link_User_Ride` (user_id ↔ ride_id),
  - `Link_Ride_Payment` (ride_id ↔ payment_id),
  - `Link_Route_Vehicle` (route_id ↔ vehicle_id),
  - `Link_Vehicle_Route_Number` (vehicle_id ↔ route_number из Kafka),
  - и т.п.

- **Satellites:**
  - `Sat_User_Main` (атрибуты пользователя, история по времени),
  - `Sat_Route_Tariff` (base_fare + история),
  - `Sat_Vehicle_Main`,
  - `Sat_Ride_Main` (start_time, end_time, fare_amount),
  - `Sat_Payment_Main` (amount, status, method),
  - `Sat_VehiclePositionEvent` (event_time, coords, speed_kmh, passengers_estimated, …).

**Плюсы:**

1. **Максимальная гибкость и расширяемость:**
   - легко добавлять новые источники (ещё одну OLTP, ещё один поток).
   - атрибуты можно добавлять новыми satellites, не ломая схему.

2. **История и lineage «из коробки»:**
   - все изменения атрибутов пишутся в satellites с временными полями.
   - удобно для очень крупных корпораций с большим количеством систем.

**Минусы для нашего кейса:**

1. **Сильно выше сложность:**
   - Очень много таблиц даже на простом домене.
   - Чтобы получить простой отчёт «выручка по маршрутам по дням», нужно:
     - джойнить hubs, links, satellites,
     - понимать, где какие атрибуты.
   - Студентам и начинающим аналитикам это тяжело.

2. **Потребуется дополнительный слой для удобной аналитики:**
   - поверх Data Vault **всё равно** часто строят **Business Vault / Kimball‑витрины** (звёзды и снежинки).
   - Получается: две модели вместо одной (Vault + Dimensional).

3. **Для одного OLTP и одного Kafka-потока — избыточно:**
   - мы не решаем проблему «сотни источников, сложный ландшафт»;
   - создаём лишнюю системную сложность, не принося дополнительной пользы в рамках учебного проекта.


#### 1.4. Сравнение по ключевым критериям

| Критерий                          | Kimball                           | Data Vault 2.0                             |
|----------------------------------|-----------------------------------|-------------------------------------------|
| Кол-во источников                | Хорошо для малого/среднего       | Силен при большом числе разнородных      |
| Сложность схемы                  | Небольшое                         | Высокая (много Hubs/Links/Sats)          |
| Порог входа для аналитиков       | Низкий                            | Высокий                                   |
| Удобство для BI / отчётов       | Отличное                          | Нужен отдельный слой витрин              |
| SCD / история                    | Реализуется вручную (SCD1/2/3)    | История является базовой концепцией      |
| Скорость реализации прототипа    | Высокая                           | Ниже (дольше моделировать/кодить)        |
| Подходит ли для MetroPulse       | Да, полностью                    | Теоретически да, но чрезмерно сложно     |

**Вывод:**

Для учебного проекта MetroPulse с одной OLTP-базой и одним Kafka-потоком **рационально выбирать подход Кимбалла (моделирование измерений)**:

- Быстрее реализовать прототип.
- Модель ясна на защите.
- Легко строить витрины и аналитические запросы.
- Можно наглядно показать SCD Type 2 на измерении `dim_route`.

## 3. Логическая модель DWH

### 3.1. Слои

1. **Staging (STG)** — сырые данные, максимально близки к источникам:
   - `stg_users`, `stg_routes`, `stg_vehicles`, `stg_rides`, `stg_payments`, `stg_vehicle_positions`.

2. **Core DWH** — очищенные, интегрированные данные:
   - Измерения `dim_*`
   - Факты `fact_*`

### 3.2. Измерения

1. **dim_user**
   - 1 строка = 1 пользователь.
   - Натуральный ключ: `user_id_nk` (из OLTP `USERS.user_id`).
   - Атрибуты: `name`, `email`, `city`, `created_at`.
   - SCD: упрощённо **Type 1** (перезапись).

2. **dim_route** (SCD Type 2)
   - 1 строка = состояние маршрута в период времени.
   - Натуральный ключ: `route_id_nk` (из `ROUTES.route_id`).
   - Атрибуты: `route_number`, `vehicle_type`, `base_fare`.
   - История: `valid_from`, `valid_to`, `is_current`.
   - Тут храним изменения тарифов/параметров.

3. **dim_vehicle**
   - 1 ТС.
   - Натуральный ключ: `vehicle_id_nk`.
   - Атрибуты: `license_plate`, `capacity`

4. **dim_datetime**
   - 1 минута
   - PK: `time_sk`.
   - Атрибуты: `ts`, `date`, `year`, `month`, `day`, `hour`, `minute`, `day_of_week`, `is_weekend`.
   - Используется `RIDES.start_time`, `RIDES.end_time`, `PAYMENTS.created_at`, `event_time` из Kafka.

### 3.3. Факты

1. **fact_ride**
   - **1 строка = 1 поездка (RIDES.ride_id)**.
   - Ключ:
     - PK: `ride_id_nk` (uuid).
   - Связи:
     - `user_sk` → `dim_user`
     - `route_sk` → `dim_route` (историческое состояние маршрута на момент начала поездки)
     - `vehicle_sk` → `dim_vehicle`
     - `start_time_sk`, `end_time_sk` → `dim_datetime`
   - Меры и атрибуты:
     - `fare_amount`
     - `ride_duration_sec` = `end_time - start_time`
     - можно добавить: `is_completed` (end_time не NULL).

2. **fact_payment**
   - **1 строка = 1 платеж (PAYMENTS.payment_id)**.
   - Ключ:
     - PK: `payment_id_nk` (uuid).
   - Связи:
     - `user_sk` → `dim_user`
     - `ride_id_nk` — как degenerate dimension / связь с fact_ride
     - `time_sk` → `dim_datetime` (по `created_at`)
   - Меры:
     - `amount`
   - Атрибуты:
     - `payment_method`
     - `status`

3. **fact_vehicle_movement** (из Kafka)
   - Batch: агрегированное окно, например: **ТС + маршрут + 5-минутный интервал**.
   - Связи:
     - `vehicle_sk` → `dim_vehicle`
     - `route_sk` → `dim_route` (по `route_number` и дате)
     - `time_sk` → `dim_datetime` (начало интервала)
   - Меры:
     - `avg_speed_kmh`
     - `max_speed_kmh`
     - `avg_passengers_estimated`
     - `events_count`
   - Эти данные нужны для:
     - средней скорости по маршрутам/часам,
     - загруженности ТС.

## 4. Ключевые таблицы 

### 4.1. Измерения

- **dim_user**
  - Хранит атрибуты пользователя.
  - Основное измерение для срезов по пользователям и городам.

- **dim_route** (SCD2)
  - Исторический справочник маршрутов.
  - Важна история изменения `base_fare` и, при необходимости, других атрибутов.

- **dim_vehicle**
  - Справочник ТС, для связки поездок и телеметрии (Kafka).

- **dim_datetime**
  - Календарь/время, для агрегации по часам/дням/месяцам, пик/не пик.

### 4.2. Факты

- **fact_ride**
  - Факт поездки.
  - Используется для аналитики по:
    - популярности маршрутов,
    - длительности поездок,
    - выручке на поездку/маршрут.

- **fact_payment**
  - Факт платежа.
  - Аналитика по суммам, статусам, методам оплаты.

- **fact_vehicle_movement**
  - Агрегированное движение ТС.
  - Исторические данные для средней скорости, загруженности, корреляции с задержками поездок.

## 5. Реализация SCD Type 2 для измерения ROUTES

**Объект:** `ROUTES` → измерение `dim_route`.

- Натуральный ключ: `route_id_nk` (из `ROUTES.route_id`).
- Атрибуты, которые могут меняться:
  - `route_number`
  - `vehicle_type`
  - `base_fare`
- Поля SCD2:
  - `valid_from` (TIMESTAMP)
  - `valid_to` (TIMESTAMP)
  - `is_current` (BOOLEAN)

**Логика:**

- При первой загрузке маршрута:
  - Вставляем строку:
    - `valid_from = дата_начала_действия` (или дата загрузки),
    - `valid_to = '9999-12-31'`,
    - `is_current = TRUE`.

- При изменении `base_fare` или других значимых атрибутов:
  1. Находим текущую строку по `route_id_nk` и `is_current = TRUE`.
  2. Обновляем её:
     - `valid_to = дата_изменения - ε`,
     - `is_current = FALSE`.
  3. Вставляем новую строку с обновлёнными значениями:
     - `valid_from = дата_изменения`,
     - `valid_to = '9999-12-31'`,
     - `is_current = TRUE`.

- При загрузке `fact_ride`:
  - Определяем `route_sk` как строку `dim_route`, у которой:
    - `route_id_nk` = `RIDES.route_id` и
    - `valid_from <= start_time < valid_to`.

Так мы гарантируем корректную стоимость/характеристики маршрута на момент каждой поездки.
