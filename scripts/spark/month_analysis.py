from pyspark.sql import SparkSession
from pyspark.sql.functions import month, count
import os

def analyze_monthly_counts(input_path, output_path):
    spark = SparkSession.builder.appName("MonthlyCounts").getOrCreate()

    try:
        df = spark.read.parquet(input_path)
        df = df.withColumn("month", month("period"))
        result = df.groupBy("month").agg(count("*").alias("pollution_count")).orderBy("month")
        result.show(truncate=False)
        result.coalesce(1).write.option("header", True).csv(output_path, mode="overwrite")
        print(f"[✓] Месячная статистика сохранена: {output_path}")
    except Exception as e:
        print(f"[!] Ошибка при обработке {input_path}: {e}")

    spark.stop()

if __name__ == "__main__":
    input_dirs = {
        "extreme": "hdfs:///opt/hadoop/clean/extreme_pollution",
        "high": "hdfs:///opt/hadoop/clean/high_pollution"
    }
    base_output_dir = os.path.expanduser("~/Desktop/water_pollution_analysis/data/seasonality_month")

    for key, path in input_dirs.items():
        out = os.path.join(base_output_dir, key)
        print(f"\n=== Месячный подсчет загрязнений: {key} ===")
        analyze_monthly_counts(path, out)
