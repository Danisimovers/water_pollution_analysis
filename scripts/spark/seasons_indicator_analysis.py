from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, avg
import os

# Создание Spark-сессии
spark = SparkSession.builder.appName("SeasonalityBySubstanceYearly").getOrCreate()

def process_pollution(hdfs_path, local_dir):
    try:
        df = spark.read.parquet(hdfs_path)
        df = df.withColumn("year", year("period")).withColumn("month", month("period"))

        df_result = df.groupBy("indicator", "year", "month") \
            .agg(avg("value_max").alias("avg_concentration")) \
            .orderBy("indicator", "year", "month")

        print(f"=== Результаты для {hdfs_path} ===")
        df_result.show(50, truncate=False)

        # Сохраняем в локальную файловую систему как CSV
        os.makedirs(local_dir, exist_ok=True)
        df_result.coalesce(1).write \
            .option("header", True) \
            .mode("overwrite") \
            .csv("file://" + local_dir)

        print(f"[✓] Сохранено в директорию: {local_dir}\n")

    except Exception as e:
        print(f"[!] Ошибка при обработке {hdfs_path}: {e}\n")

# Пути
base_output = "/home/Eugene/Desktop/water_pollution_analysis/data/analysed_data_csv"

datasets = {
    "extreme": {
        "hdfs": "hdfs://localhost:9000/opt/hadoop/clean/extreme_pollution",
        "local": os.path.join(base_output, "extreme", "seasonality_by_substance_year_month")
    },
    "high": {
        "hdfs": "hdfs://localhost:9000/opt/hadoop/clean/high_pollution",
        "local": os.path.join(base_output, "high", "seasonality_by_substance_year_month")
    }
}

for label, paths in datasets.items():
    process_pollution(paths["hdfs"], paths["local"])
