import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

def process_seasonality_year(base_path, output_base_path):
    spark = SparkSession.builder.appName("SeasonalityByYear").getOrCreate()

    for category in ["extreme", "high"]:
        input_path = os.path.join(base_path, f"{category}_pollution")
        print(f"Обработка: {input_path}")

        try:
            df = spark.read.parquet(input_path)

            # Добавим колонку year из period
            df = df.withColumn("year", year("period"))

            # Группировка по году с подсчётом кол-ва случаев загрязнений
            result = df.groupBy("year").agg(count("*").alias("pollution_count")).orderBy("year")

            # Локальный путь для сохранения
            local_output_dir = os.path.expanduser(os.path.join(output_base_path, category, "seasonality_year"))
            os.makedirs(local_output_dir, exist_ok=True)
            local_output_path = "file://" + local_output_dir

            print(f"Сохраняем результат в {local_output_path}")
            result.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_output_path)

            # Вывод в консоль
            result.show()

        except Exception as e:
            print(f"[!] Ошибка при обработке {category}: {e}")

    spark.stop()

if __name__ == "__main__":
    base_path = "/opt/hadoop/clean"
    output_base_path = "~/Desktop/water_pollution_analysis/data/analysed_data_csv"
    process_seasonality_year(base_path, output_base_path)
