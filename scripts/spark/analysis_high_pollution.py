from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, when

spark = SparkSession.builder.appName("HighPollutionAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

high_path = "/opt/hadoop/clean/high_pollution"
pdk_path = "/opt/hadoop/clean/pdk"
save_path = "/opt/hadoop/analysed_data/high"

df_high = spark.read.parquet(high_path)
df_pdk = spark.read.parquet(pdk_path)

df_pdk = df_pdk.select(
    col("ingredient").alias("indicator"),
    col("value").alias("pdk_value")
)

df_high_joined = df_high.join(df_pdk, on="indicator", how="left")

# 1. Средняя концентрация
df_avg_conc = df_high.groupBy("indicator") \
    .agg(avg("value_max").alias("avg_concentration")) \
    .orderBy(col("avg_concentration").desc())

print("=== Средняя концентрация каждого вещества (high) ===")
df_avg_conc.show(10, False)

# 2. ТОП-10 по частоте превышения ПДК
df_high_pdk = df_high_joined.withColumn(
    "exceeded", when(col("value_max") > col("pdk_value"), 1).otherwise(0)
)

df_top10_exceed = df_high_pdk.groupBy("indicator").agg(
    count("*").alias("total"),
    count(when(col("exceeded") == 1, True)).alias("exceeds")
).withColumn("exceed_ratio", col("exceeds") / col("total")) \
 .orderBy(col("exceeds").desc()) \
 .limit(10)

print("=== ТОП-10 веществ по частоте превышения ПДК (high) ===")
df_top10_exceed.show(10, False)

# 3. Доля записей с превышением ПДК
df_share_exceed = df_high_pdk.select(
    (count(when(col("exceeded") == 1, True)) / count("*")).alias("share_exceeding_pdk")
)

print("=== Доля записей, где ПДК превышена (high) ===")
df_share_exceed.show()

# Сохраняем в Parquet
df_avg_conc.coalesce(1).write.mode("overwrite").parquet(save_path + "/avg_concentration")
df_top10_exceed.coalesce(1).write.mode("overwrite").parquet(save_path + "/top10_exceed")
df_share_exceed.coalesce(1).write.mode("overwrite").parquet(save_path + "/share_exceeding_pdk")

spark.stop()

