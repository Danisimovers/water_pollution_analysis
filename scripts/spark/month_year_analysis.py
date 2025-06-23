import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count

def process_seasonality_month_year(base_path, output_base_path):
    spark = SparkSession.builder.appName("SeasonalityByMonthYear").getOrCreate()

    for category in ["extreme", "high"]:
        input_path = os.path.join(base_path, f"{category}_pollution")
        print(f"Обработка: {input_path}")

        try:
            df = spark.read.parquet(input_path)

            # Добавим колонки year и month из period
            df = df.withColumn("year", year("period")).withColumn("month", month("period"))

            # Группировка по году и месяцу
            result = df.groupBy("year", "month").agg(count("*").alias("pollution_count")).orderBy("year", "month")

            # Локальный путь
            local_output_dir = os.path.expanduser(os.path.join(output_base_path, category, "seasonality_month_year"))
            os.makedirs(local_output_dir, exist_ok=True)
            local_output_path = "file://" + local_output_dir

            print(f"Сохраняем результат в {local_output_path}")
            result.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_output_path)

            result.show()

        except Exception as e:
            print(f"[!] Ошибка при обработке {category}: {e}")

    spark.stop()

if __name__ == "__main__":
    base_path = "/opt/hadoop/clean"
    output_base_path = "~/Desktop/water_pollution_analysis/data/analysed_data_csv"
    process_seasonality_month_year(base_path, output_base_path)
