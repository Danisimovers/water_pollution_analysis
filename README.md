# Water Pollution Analysis

Проект по анализу загрязнения водных ресурсов с использованием данных о превышениях ПДК, географической информации и состава загрязнителей. Используются инструменты Spark, Sqoop, MariaDB и Power BI для обработки, анализа и визуализации данных.

## 📁 Структура проекта

- `data/raw/` — исходные CSV-файлы и метаданные (`*.csv`, структура, ПДК).
- `data/clean/` — очищенные данные после предварительной обработки.
- `data/analysed_data/` — результаты анализа в формате Parquet:
  - `extreme/`, `high/` — разбивка по степени загрязнения;
  - `avg_concentration/`, `region_heatmap/`, `top10_*`, и др.
- `scripts/` — скрипты:
  - `spark/` — анализ и очистка на PySpark;
  - `sqoop/` — импорты из HDFS в MariaDB;
  - `mariadb/` — SQL-таблицы для хранения.
- `docs/` — документация проекта (в т.ч. план).
- `samples/` — укороченные версии файлов для отладки.
- `README.md` — описание проекта.

## ⚙️ Используемые технологии

- Apache Spark (PySpark)
- Apache Sqoop
- MariaDB
- Power BI (веб-версия)
- Python 3.x
- Parquet

## 🚀 Как запустить

1. Установите Spark, Sqoop и MariaDB.
2. Импортируйте данные:
    ```bash
    bash scripts/sqoop/import_extreme_pollution.sh
    ```
3. Очистка и предобработка:
    ```bash
    spark-submit scripts/spark/clean_water_pollution.py
    ```
4. Анализ:
    ```bash
    spark-submit scripts/spark/analysis_extreme_pollution.py
    spark-submit scripts/spark/analysis_high_pollution.py
    ```

## 📊 Визуализация

Результаты визуализируются в **Power BI** (веб-версия), через:
- Прямое подключение к MariaDB
- Или загрузку `.parquet`/`.csv` из `data/analysed_data/`

Метрики:
- Средние концентрации загрязняющих веществ
- Доли и частота превышения ПДК
- Географические тепловые карты
- ТОП-10 загрязнённых регионов и водоёмов

## 📌 Источники данных

Описание источников и происхождения данных: [`data/raw_sources.md`](data/raw_sources.md)

## 📅 План проекта

План и структура этапов: [`docs/project_plan.md`](docs/project_plan.md)

## 🧑‍💻 Автор

- Данил @danil-GF63
