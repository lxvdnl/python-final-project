# python-final-project
Финальное групповое задание по курсу "Python для инженерии данных

Проект поднимает мультиконтейнерное приложение с:
- PostgreSQL — основная база данных для нормализованных таблиц и витрин;
- pgAdmin — интерфейс для просмотра БД и выполнения SQL-запросов;
- Apache Airflow — оркестратор ETL/ELT-процессов.

## Запуск

Из корня проекта:

```bash
docker compose up -d --build
```

Для сброса сервисов вместе с кешем:

```bash
docker-compose down -v --remove-orphans
```

## Сервисы

| Сервис       | URL                     | Логин                | Пароль             |
|--------------|-------------------------|----------------------|--------------------|
| Airflow      | http://localhost:8080   | admin                | admin              |
| pgAdmin      | http://localhost:5050   | admin@example.com    | admin              |
| PostgreSQL   | localhost:5432          | project_user         | project_password   |

## Подключение к БД (pgAdmin)

Host: postgres  
Port: 5432  
Database: project_db  
User: project_user  
Password: project_password  

## Структура проекта

```text
python-final-project/
├── airflow/dags      # DAG-и Airflow
├── airflow/sripts    # Скрипты для Airflow
├── data/raw          # parquet-файлы
├── sql/init          # SQL-скрипт создания БД
├── pgadmin           # файл для настройки pgadmin
├── docker-compose.yml
└── README.md
```

##  Что происходит при старте
Поднимается контейнер PostgreSQL.

Автоматически создается база project_db.

Автоматически выполняется SQL-скрипт и создаются таблицы.

Поднимаются контейнеры Airflow.

В pgAdmin уже преднастроен сервер для подключения к PostgreSQL.

##  Порядок запуска DAG-ов
После старта контейнеров откройте Airflow и выполните DAG-и в таком порядке:

load_normalized_data

quality_checks

orders_datamart

items_datamart

##  Описание DAG-ов
### load_normalized_data
Загружает исходные parquet-файлы, преобразует данные и записывает их в нормализованные таблицы PostgreSQL.
```text
Что делает:

- читает сырые parquet-файлы;

- формирует таблицы users, stores, drivers, items, orders, order_items, delivery_assignments;

- нормализует телефоны пользователей и курьеров;

- загружает данные в PostgreSQL.
```

### quality_checks
Проверяет качество уже загруженных данных.

```text
Что проверяет:

- таблицы не пустые;

- нет сиротских записей в order_items;

- нет заказов без пользователей и магазинов;

- нет невалидных ссылок в replaced_by_item_id;

- нет дублей по логической группе позиции заказа;

- нет невалидных интервалов дат в delivery_assignments;

- телефоны содержат только цифры после нормализации.
```