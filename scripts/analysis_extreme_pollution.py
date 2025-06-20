from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, when

spark = SparkSession.builder.appName("ExtremePollutionAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Отключаем лишние логи

extreme_path = "/opt/hadoop/clean/extreme_pollution"
pdk_path = "/opt/hadoop/clean/pdk"
save_path = "/opt/hadoop/analysed_data/extreme"

df_extreme = spark.read.parquet(extreme_path)
df_pdk = spark.read.parquet(pdk_path)

df_pdk = df_pdk.select(
    col("ingredient").alias("indicator"),
    col("value").alias("pdk_value")
)

df_extreme_joined = df_extreme.join(df_pdk, on="indicator", how="left")

# 1. Средняя концентрация каждого вещества
df_avg_conc = df_extreme.groupBy("indicator") \
    .agg(avg("value_max").alias("avg_concentration")) \
    .orderBy(col("avg_concentration").desc())

print("=== Средняя концентрация каждого вещества (extreme) ===")
df_avg_conc.show(10, False)

# 2. ТОП-10 веществ по частоте превышения ПДК
df_extreme_pdk = df_extreme_joined.withColumn(
    "exceeded", when(col("value_max") > col("pdk_value"), 1).otherwise(0)
)

df_top10_exceed = df_extreme_pdk.groupBy("indicator").agg(
    count("*").alias("total"),
    count(when(col("exceeded") == 1, True)).alias("exceeds")
).withColumn("exceed_ratio", col("exceeds") / col("total")) \
 .orderBy(col("exceeds").desc()) \
 .limit(10)

print("=== ТОП-10 веществ по частоте превышения ПДК (extreme) ===")
df_top10_exceed.show(10, False)

# 3. Доля записей, где ПДК превышена
df_share_exceed = df_extreme_pdk.select(
    (count(when(col("exceeded") == 1, True)) / count("*")).alias("share_exceeding_pdk")
)

print("=== Доля записей, где ПДК превышена (extreme) ===")
df_share_exceed.show()

# Сохраняем в HDFS
df_avg_conc.coalesce(1).write.mode("overwrite").csv(save_path + "/avg_concentration", header=True)
df_top10_exceed.coalesce(1).write.mode("overwrite").csv(save_path + "/top10_exceed", header=True)
df_share_exceed.coalesce(1).write.mode("overwrite").csv(save_path + "/share_exceeding_pdk", header=True)

spark.stop()
