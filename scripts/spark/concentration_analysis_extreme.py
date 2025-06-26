from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("NormalizeExtremePollution").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

extreme_path = "/opt/hadoop/clean/extreme_pollution"
pdk_path = "/opt/hadoop/clean/pdk"
output_base = "file:///home/Eugene/Desktop/water_pollution_analysis/data/analysed_data_csv/extreme"

df_extreme = spark.read.parquet(extreme_path)
df_pdk_raw = spark.read.parquet(pdk_path)

df_pdk = df_pdk_raw.select(
    col("ingredient").alias("indicator"),
    col("hazard_class").alias("pdk_hazard_class"),
    col("value").alias("pdk_value"),
    col("date_start"),
    col("date_end"),
    col("npa").alias("ingredient_npa")
).withColumn("date_start", col("date_start").cast(DateType())) \
 .withColumn("date_end", col("date_end").cast(DateType()))

df_extreme = df_extreme.withColumn("period", col("period").cast(DateType()))

df_joined = df_extreme.join(
    df_pdk,
    (df_extreme.indicator == df_pdk.indicator) &
    (df_extreme.hazard_class == df_pdk.pdk_hazard_class),
    how="left"
)

df_joined = df_joined.withColumn(
    "in_period",
    (col("period") >= col("date_start")) & (col("period") <= col("date_end"))
)

window = Window.partitionBy("id").orderBy(col("in_period").desc(), col("date_start").desc())

df_ranked = df_joined.withColumn("rank", row_number().over(window))

# Берём только первую запись для каждого загрязнения
df_best = df_ranked.filter(col("rank") == 1).drop("rank")

# Добавляем колонку с концентрацией в мг/л
df_final = df_best.withColumn(
    "concentration_mg_l",
    when(col("unit") == "ПДК", col("value_max") * col("pdk_value"))
    .when(col("unit") == "мг/л", col("value_max"))
    .otherwise(None)
)

# Отдельный датасет для строк с unit ПДК или мг/л и NULL concentration_mg_l
df_no_conc = df_final.filter(
    (col("unit").isin("ПДК", "мг/л")) & (col("concentration_mg_l").isNull())
)

# Основной датасет концентраций (без строк с пустой концентрацией)
df_conc = df_final.filter(
    (col("unit").isin("ПДК", "мг/л")) & (~col("concentration_mg_l").isNull())
)

# Отдельно pH
df_ph = df_final.filter(col("unit") == "pH")

# Отдельно запах (балл)
df_smell = df_final.filter(col("unit") == "балл")

# Записываем CSV локально
df_conc.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/concentration_mg_l")
df_no_conc.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/concentration_mg_l_missing")
df_ph.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/ph_values")
df_smell.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/smell_values")
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/final_all_columns")

spark.stop()
