from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, min as _min, when, count, lit)

spark = (SparkSession.builder
         .appName("WaterPollutionGeoAnalysis")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# === Пути ===================================================================
base_in  = "hdfs://localhost:9000/user/danil/water_pollution/clean"
base_out = "hdfs://localhost:9000/user/danil/water_pollution/analysed_data"

paths = {
    "high":    f"{base_in}/high_pollution",
    "extreme": f"{base_in}/extreme_pollution",
    "pdk":     f"{base_in}/pdk"
}

# === Справочник ПДК ==========================================================
pdk = (spark.read.parquet(paths["pdk"])
              .groupBy("ingredient")
              .agg(_min("value").alias("pdk_value"))
              .withColumnRenamed("ingredient", "indicator"))

def analyse(kind: str, region_col: str, water_col: str) -> None:
    title = "высокого" if kind == "high" else "экстремального"
    kind_label = "high_pollution" if kind == "high" else "extreme_pollution"
    
    print(f"\n{'═'*50}\nАнализ {title} загрязнения ({kind_label})\n{'═'*50}")

    df = (spark.read.parquet(paths[kind])
            .join(pdk, "indicator", "left")
            .withColumn(
                "exceed_ratio",
                when(col("pdk_value").isNotNull(),
                     col("value_max") / col("pdk_value"))
            )  # если ПДК нет → NULL
         )

    # ---------- регионы ------------------------------------------------------
    region_stats = (df.groupBy(region_col)
                      .agg(
                          _sum("cnt_cases").alias("total_cases"),
                          avg("value_max").alias("avg_concentration"),
                          avg(col("exceed_ratio")).alias("avg_exceed_ratio"),
                          count("*").alias("records"))
                      .withColumn(
                          "avg_exceed_ratio",
                          when(col("avg_exceed_ratio").isNull(), lit(0))
                               .otherwise(col("avg_exceed_ratio")))
                   )

    # ТОП-10 по частоте / степени
    print(f"\nТОП-10 регионов по частоте ({kind_label}) → top10_regions_freq")
    region_stats.orderBy(col("total_cases").desc()).limit(10).show(truncate=False)
    region_stats.orderBy(col("total_cases").desc()).limit(10).write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/top10_regions_freq")

    print(f"\nТОП-10 регионов по средней степени превышения ПДК ({kind_label}) → top10_regions_degree")
    region_stats.orderBy(col("avg_exceed_ratio").desc()).limit(10).show(truncate=False)
    region_stats.orderBy(col("avg_exceed_ratio").desc()).limit(10).write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/top10_regions_degree")

    print(f"\nТепловая карта загрязнённости по регионам (где превышение PDK > 0) → region_heatmap ({kind_label})")
    region_stats.filter(col("avg_exceed_ratio") > 0) \
                .orderBy(col("avg_exceed_ratio").desc()) \
                .show(20, truncate=False)
    region_stats.write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/region_heatmap")

    # ---------- водоёмы ------------------------------------------------------
    water_stats = (df.groupBy(water_col)
                     .agg(
                         _sum("cnt_cases").alias("total_cases"),
                         avg("value_max").alias("avg_concentration"),
                         avg(col("exceed_ratio")).alias("avg_exceed_ratio"),
                         count("*").alias("records"))
                     .withColumn(
                         "avg_exceed_ratio",
                         when(col("avg_exceed_ratio").isNull(), lit(0))
                              .otherwise(col("avg_exceed_ratio")))
                  )

    print(f"\nТОП-10 водоёмов по частоте ({kind_label}) → top10_water_freq")
    water_stats.orderBy(col("total_cases").desc()).limit(10).show(truncate=False)
    water_stats.orderBy(col("total_cases").desc()).limit(10).write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/top10_water_freq")

    print(f"\nТОП-10 водоёмов по средней степени превышения ПДК ({kind_label}) → top10_water_degree")
    water_stats.orderBy(col("avg_exceed_ratio").desc()).limit(10).show(truncate=False)
    water_stats.orderBy(col("avg_exceed_ratio").desc()).limit(10).write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/top10_water_degree")

    print(f"\nТепловая карта загрязнённости по водоёмам (где превышение PDK > 0) → water_heatmap ({kind_label})")
    water_stats.filter(col("avg_exceed_ratio") > 0) \
               .orderBy(col("avg_exceed_ratio").desc()) \
               .show(20, truncate=False)
    water_stats.write.mode("overwrite") \
        .parquet(f"{base_out}/{kind}/water_heatmap")

    # Сводка
    print(f"\n✔ {kind_label}: обработано {df.count():,} записей "
          f"· {region_stats.count()} регионов · {water_stats.count()} водоёмов\n")

# === Запуск =================================================================
analyse("high",    region_col="subject", water_col="river_basin")
analyse("extreme", region_col="subject", water_col="water_body")
spark.stop()
