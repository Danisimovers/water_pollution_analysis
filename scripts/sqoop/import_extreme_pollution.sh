sqoop import \
  --connect jdbc:mysql://localhost:3306/water_pollution \
  --username danil \
  --password password \
  --table extreme_pollution \
  --target-dir /user/danil/water_pollution/extreme_pollution \
  --delete-target-dir \
  --num-mappers 1 \
  --as-parquetfile
