import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, count

def process_seasonality_month(base_path, output_base_path):
    spark = SparkSession.builder.appName("SeasonalityByMonth").getOrCreate()

    for category in ["extreme", "high"]:
        input_path = os.path.join(base_path, f"{category}_pollution")
        print(f"Обработка: {input_path}")

        try:
            df = spark.read.parquet(input_path)

            # Добавим колонку month из period
            df = df.withColumn("month", month("period"))

            # Группировка по месяцу с подсчётом кол-ва случаев загрязнений
            result = df.groupBy("month").agg(count("*").alias("pollution_count")).orderBy("month")

            # Формируем локальный путь с file://
            local_output_dir = os.path.expanduser(os.path.join(output_base_path, category, "seasonality_month"))
            os.makedirs(local_output_dir, exist_ok=True)
            local_output_path = "file://" + local_output_dir

            print(f"Сохраняем результат в {local_output_path}")
            result.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_output_path)

            # Также вывод в консоль
            result.show()

        except Exception as e:
            print(f"[!] Ошибка при обработке {category}: {e}")

    spark.stop()

if __name__ == "__main__":
    base_path = "/opt/hadoop/clean"  # Путь к parquet в HDFS
    output_base_path = "~/Desktop/water_pollution_analysis/data/analysed_data_csv"
    process_seasonality_month(base_path, output_base_path)
