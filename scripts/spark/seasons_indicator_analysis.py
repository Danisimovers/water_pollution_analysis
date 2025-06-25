from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, avg
import os

# Запуск Spark-сессии
spark = SparkSession.builder.appName("SeasonalityBySubstanceYearly").getOrCreate()

# Обработка одного датасета
def process_pollution(hdfs_path, local_filename):
    try:
        # Загрузка данных из HDFS
        df = spark.read.parquet(hdfs_path)

        # Добавление года и месяца
        df = df.withColumn("year", year("period")).withColumn("month", month("period"))

        # Группировка по году, месяцу и веществу, расчёт средней концентрации
        df_result = df.groupBy("indicator", "year", "month") \
            .agg(avg("value_max").alias("avg_concentration")) \
            .orderBy("indicator", "year", "month")

        # Показать первые 50 строк в консоли
        print(f"=== Результаты для {hdfs_path} ===")
        df_result.show(50, truncate=False)

        # Сохранение на локальный диск
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        df_result.toPandas().to_csv(local_filename, index=False)
        print(f"[✓] Сохранено: {local_filename}\n")

    except Exception as e:
        print(f"[!] Ошибка при обработке {hdfs_path}: {e}\n")

# Пути
base_output = "/home/Eugene/Desktop/water_pollution_analysis/data/analysed_data_csv"

datasets = {
    "extreme": {
        "hdfs": "hdfs://localhost:9000/opt/hadoop/clean/extreme_pollution",
        "local": os.path.join(base_output, "extreme", "seasonality_by_substance_year_month.csv")
    },
    "high": {
        "hdfs": "hdfs://localhost:9000/opt/hadoop/clean/high_pollution",
        "local": os.path.join(base_output, "high", "seasonality_by_substance_year_month.csv")
    }
}

# Запуск обработки для каждого датасета
for label, paths in datasets.items():
    process_pollution(paths["hdfs"], paths["local"])
