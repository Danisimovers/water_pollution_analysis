from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count
import os

def analyze_year_month_counts(input_path, output_path):
    spark = SparkSession.builder.appName("YearMonthCounts").getOrCreate()

    try:
        df = spark.read.parquet(input_path)
        df = df.withColumn("year", year("period")).withColumn("month", month("period"))
        result = df.groupBy("year", "month").agg(count("*").alias("pollution_count")).orderBy("year", "month")
        result.show(truncate=False)
        result.coalesce(1).write.option("header", True).csv(output_path, mode="overwrite")
        print(f"[✓] Статистика по годам и месяцам сохранена: {output_path}")
    except Exception as e:
        print(f"[!] Ошибка при обработке {input_path}: {e}")

    spark.stop()

if __name__ == "__main__":
    input_dirs = {
        "extreme": "hdfs:///opt/hadoop/clean/extreme_pollution",
        "high": "hdfs:///opt/hadoop/clean/high_pollution"
    }
    base_output_dir = os.path.expanduser("~/Desktop/water_pollution_analysis/data/seasonality_year_month")

    for key, path in input_dirs.items():
        out = os.path.join(base_output_dir, key)
        print(f"\n=== Подсчет по годам и месяцам: {key} ===")
        analyze_year_month_counts(path, out)
