# python-final-project
Финальное групповое задание по курсу "Python для инженерии данных

## Запуск

Из корня проекта:

```bash
docker compose up -d --build
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
├── data/raw          # parquet-файлы
├── sql               # SQL-скрипты
├── docker-compose.yml
└── README.md
```

> Примечание: в `etl_pipeline.py` сейчас лежит пустой DAG для теста подключения к airflow.