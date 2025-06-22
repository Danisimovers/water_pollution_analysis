from pyspark.sql import SparkSession
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # до water_pollution_analysis
input_base = BASE_DIR / "data" / "analysed_data"
output_base = BASE_DIR / "data" / "analysed_data_csv"

spark = SparkSession.builder \
    .appName("ParquetToCSV") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def convert_parquet_to_csv(input_dir, output_dir):
    try:
        df = spark.read.parquet(str(input_dir))
        output_dir.mkdir(parents=True, exist_ok=True)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_dir))
        print(f"[OK] CSV сохранён: {output_dir}")
    except Exception as e:
        print(f"[Ошибка] {input_dir}: {e}")

for root, dirs, files in os.walk(input_base):
    for d in dirs:
        in_path = Path(root) / d
        rel_path = in_path.relative_to(input_base)
        out_path = output_base / rel_path
        print(f"Обработка: {in_path}")
        convert_parquet_to_csv(in_path, out_path)

spark.stop()
