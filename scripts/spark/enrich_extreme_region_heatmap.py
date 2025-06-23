from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("EnrichExtremeHeatmapWithGeo").getOrCreate()

# Пути к файлам
heatmap_csv = "data/analysed_data_csv/extreme/region_heatmap/extreme_region_heatmap.csv"
clean_parquet = "data/clean/extreme_pollution/part-00000-a33f8411-5dde-48ab-aa4d-b64ad14d0796-c000.snappy.parquet"
out_path = "data/analysed_data_csv/extreme/region_heatmap_enriched"

# Загружаем heatmap
df_heat = (
    spark.read.option("header", "true")
              .option("delimiter", ",")   # проверь, если табы; убери, если запятые
              .csv(heatmap_csv)
)

# Загружаем чистые измерения
df_clean = spark.read.parquet(clean_parquet)

# Оставляем нужные колонки и чистим
geo_cols = ["subject", "municipality", "settlement", "oktmo"]
df_geo = (
    df_clean.select(*geo_cols)
            .dropna(subset=["subject"])
            .dropDuplicates()
)

# Агрегируем по субъекту
df_geo_agg = (
    df_geo.groupBy("subject")
          .agg(F.collect_set("municipality").alias("municipalities"),
               F.collect_set("settlement").alias("settlements"),
               F.collect_set("oktmo").alias("oktmolist"))
)

# Джойним с heatmap
df_enriched = df_heat.join(df_geo_agg, on="subject", how="left")

# Сохраняем
(df_enriched
    .repartition(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(out_path)
)

spark.stop()
