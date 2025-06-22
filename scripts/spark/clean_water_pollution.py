#!/usr/bin/env python3
"""
clean_water_pollution.py
-----------------------------------
• Читает parquet‑таблицы extreme_pollution / high_pollution / pdk из HDFS  
• Обрезает пробелы, приводит типы (в том числе hazard_class, с заменой пустых значений на "0")  
• Удаляет ВСЕ строки, где хоть в ОДНОЙ строковой колонке встречается:
    ‑ NULL
    ‑ пустая строка ""
    ‑ строка "NaN"
• Сохраняет результат в HDFS в /user/danil/water_pollution/clean  
• Печатает сводку: всего / плохих / удалено / осталось

Запуск из корня проекта:

    spark-submit scripts/spark/clean_water_pollution.py
"""
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, from_unixtime, when

RAW_ROOT   = "hdfs://localhost:9000/user/danil/water_pollution"
CLEAN_ROOT = "hdfs://localhost:9000/user/danil/water_pollution/clean"

# Инициализация SparkSession
spark = (
    SparkSession.builder
    .appName("clean_water_pollution")
    .getOrCreate()
)

# ───────────────────────────── УТИЛИТЫ ─────────────────────────────
def read(tbl):
    """Чтение parquet-таблицы из HDFS по имени"""
    return spark.read.parquet(f"{RAW_ROOT}/{tbl}")

def write(df, tbl):
    """
    Запись DataFrame в parquet в HDFS, с repartition(1), чтобы получить один файл.
    Режим перезаписи.
    """
    (df.repartition(1)
       .write.mode("overwrite")
       .parquet(f"{CLEAN_ROOT}/{tbl}"))

def trim_all(df):
    """Обрезать пробелы (trim) во всех строковых колонках"""
    for c, t in df.dtypes:
        if t.startswith("string"):
            df = df.withColumn(c, trim(col(c)))
    return df

def drop_bad_rows(df, exclude_cols=None):
    """
    Удаляет строки, где в ЛЮБОЙ строковой колонке (кроме exclude_cols, если указаны) встречается:
        NULL | пустая строка "" | строка "NaN"
    Возвращает (очищенный DataFrame, количество удалённых строк)
    """
    if exclude_cols is None:
        exclude_cols = []

    # выбираем строковые колонки, игнорируя exclude_cols
    string_cols = [c for c, t in df.dtypes if t.startswith("string") and c not in exclude_cols]
    if not string_cols:  # если нет строковых колонок для проверки
        return df, 0

    # Условие, когда в колонке плохое значение
    bad_cond = reduce(
        lambda a, b: a | b,
        [(col(c).isNull()) | (trim(col(c)) == "") | (col(c) == "NaN") for c in string_cols]
    )

    bad_cnt = df.filter(bad_cond).count()    # сколько строк под удаление
    return df.filter(~bad_cond), bad_cnt     # возвращаем отфильтрованный df и количество плохих

def clean(df, hazard_class_fix=False):
    """
    Основная функция очистки:
    • Считает общее число строк
    • Обрезает пробелы в строках
    • Если hazard_class_fix=True, заменяет NULL и пустые строки в колонке hazard_class на "0"
    • Удаляет строки с плохими значениями (NULL, "", "NaN") в строковых колонках,
      при этом исключая hazard_class из проверки, если hazard_class_fix=True (чтобы строки с пустым hazard_class не удалялись)
    Возвращает (очищенный DataFrame, всего строк, плохих, удалённых)
    """
    total = df.count()
    df = trim_all(df)

    if hazard_class_fix:
        # Заменяем NULL и пустые строки в hazard_class на "0"
        df = df.withColumn(
            "hazard_class",
            when(col("hazard_class").isNull() | (trim(col("hazard_class")) == ""), "0")
            .otherwise(col("hazard_class"))
        )

    exclude_cols = ["hazard_class"] if hazard_class_fix else []
    df, bad_cnt = drop_bad_rows(df, exclude_cols=exclude_cols)

    dropped = bad_cnt
    return df, total, bad_cnt, dropped

# ───────────────────────────── ОЧИСТКА ─────────────────────────────
# extreme_pollution — преобразуем поля, применяем очистку с фиксом hazard_class
ext, ext_total, ext_bad, ext_drop = clean(
    read("extreme_pollution")
        .withColumn("period",       to_date(from_unixtime(col("period") / 1000)))
        .withColumn("cnt_cases",    col("cnt_cases").cast("int"))
        .withColumn("value_min",    col("value_min").cast("double"))
        .withColumn("value_max",    col("value_max").cast("double")),
    hazard_class_fix=True
)
write(ext, "extreme_pollution")

# high_pollution — аналогично extreme_pollution
high, high_total, high_bad, high_drop = clean(
    read("high_pollution")
        .withColumn("period",       to_date(from_unixtime(col("period") / 1000)))
        .withColumn("cnt_cases",    col("cnt_cases").cast("int"))
        .withColumn("value_min",    col("value_min").cast("double"))
        .withColumn("value_max",    col("value_max").cast("double")),
    hazard_class_fix=True
)
write(high, "high_pollution")

# pdk — здесь hazard_class отсутствует, просто кастим типы и очищаем без фикса hazard_class
pdk, pdk_total, pdk_bad, pdk_drop = clean(
    read("pdk")
        .withColumn("value",        col("value").cast("double"))
        .withColumn("date_start",   to_date(from_unixtime(col("date_start") / 1000)))
        .withColumn("date_end",     to_date(from_unixtime(col("date_end") / 1000)))
)
write(pdk, "pdk")

# ───────────────────────────── ОТЧЁТ ─────────────────────────────
print("\n===== РЕЗУЛЬТАТ ОЧИСТКИ =====")
for name, t, b, d in [
        ("extreme_pollution", ext_total,  ext_bad,  ext_drop),
        ("high_pollution",    high_total, high_bad, high_drop),
        ("pdk",               pdk_total,  pdk_bad,  pdk_drop)]:
    print(f"{name:18}: всего {t:>6} | плохих {b:>5} | удалено {d:>5} | осталось {t-d:>6}")

print("✓ Данные сохранены в", CLEAN_ROOT)

spark.stop()
