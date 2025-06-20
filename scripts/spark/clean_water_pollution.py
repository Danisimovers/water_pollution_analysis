#!/usr/bin/env python3
"""
clean_water_pollution.py
-----------------------------------
• Читает parquet‑таблицы extreme_pollution / high_pollution / pdk из HDFS  
• Обрезает пробелы, приводит типы  
• Удаляет ВСЕ строки, где хоть в ОДНОЙ строковой колонке встречается:
    ‑ NULL
    ‑ пустая строка ""
    ‑ строка "NaN"
• Сохраняет результат в HDFS в /user/danil/water_pollution/clean  
• Печатает сводку: всего / плохих / удалено / осталось
Запуск из корня проекта, мастер подтянется из SPARK + Hadoop конфигов:

    spark-submit scripts/spark/clean_water_pollution.py
"""
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, from_unixtime

RAW_ROOT   = "hdfs://localhost:9000/user/danil/water_pollution"
CLEAN_ROOT = "hdfs://localhost:9000/user/danil/water_pollution/clean"

spark = (SparkSession.builder
         .appName("clean_water_pollution")
         .getOrCreate())

# ────────────────────────────────────────────── утилиты ─────────────────────────────────────────────
def read(tbl):        # → DataFrame
    return spark.read.parquet(f"{RAW_ROOT}/{tbl}")

def write(df, tbl):   # перезаписываем результат в одну партицию (удобно смотреть)
    (df.repartition(1)
       .write.mode("overwrite")
       .parquet(f"{CLEAN_ROOT}/{tbl}"))

def trim_all(df):
    """trim для всех string‑колонок"""
    for c, t in df.dtypes:
        if t.startswith("string"):
            df = df.withColumn(c, trim(col(c)))
    return df

def drop_bad_rows(df):
    """
    Удаляет строки, где в ЛЮБОЙ строковой колонке:
        NULL  |  ""  |  "NaN"
    Возвращает (очищенный_df, кол‑во_плохих_строк)
    """
    string_cols = [c for c, t in df.dtypes if t.startswith("string")]
    if not string_cols:                       # нет строковых колонок
        return df, 0

    bad_cond = reduce(
        lambda a, b: a | b,
        [(col(c).isNull()) | (trim(col(c)) == "") | (col(c) == "NaN") for c in string_cols]
    )

    bad_cnt = df.filter(bad_cond).count()
    return df.filter(~bad_cond), bad_cnt

def clean(df):
    total = df.count()
    df = trim_all(df)
    df, bad_cnt = drop_bad_rows(df)
    dropped = bad_cnt                   # = сколько ушло
    return df, total, bad_cnt, dropped

# ───────────────────────────────────────── таблицы ──────────────────────────────────────────────────
ext, ext_total, ext_bad, ext_drop = clean(
    read("extreme_pollution")
        .withColumn("period",       to_date(from_unixtime(col("period") / 1000)))
        .withColumn("hazard_class", col("hazard_class").cast("int"))
        .withColumn("cnt_cases",    col("cnt_cases").cast("int"))
        .withColumn("value_min",    col("value_min").cast("double"))
        .withColumn("value_max",    col("value_max").cast("double"))
)
write(ext, "extreme_pollution")

high, high_total, high_bad, high_drop = clean(
    read("high_pollution")
        .withColumn("period",       to_date(from_unixtime(col("period") / 1000)))
        .withColumn("hazard_class", col("hazard_class").cast("int"))
        .withColumn("cnt_cases",    col("cnt_cases").cast("int"))
        .withColumn("value_min",    col("value_min").cast("double"))
        .withColumn("value_max",    col("value_max").cast("double"))
)
write(high, "high_pollution")

pdk, pdk_total, pdk_bad, pdk_drop = clean(
    read("pdk")
        .withColumn("hazard_class", col("hazard_class").cast("int"))
        .withColumn("value",        col("value").cast("double"))
        .withColumn("date_start",   to_date(from_unixtime(col("date_start") / 1000)))
        .withColumn("date_end",     to_date(from_unixtime(col("date_end") / 1000)))
)
write(pdk, "pdk")

# ────────────────────────────────────────── отчёт ──────────────────────────────────────────────────
print("\n===== РЕЗУЛЬТАТ ОЧИСТКИ =====")
for name, t, b, d in [
        ("extreme_pollution", ext_total,  ext_bad,  ext_drop),
        ("high_pollution",    high_total, high_bad, high_drop),
        ("pdk",               pdk_total,  pdk_bad,  pdk_drop)]:
    print(f"{name:18}: всего {t:>6} | плохих {b:>5} | удалено {d:>5} | осталось {t-d:>6}")

print("✓ Данные сохранены в", CLEAN_ROOT)

spark.stop()
