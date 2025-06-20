#!/usr/bin/env bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/water_pollution \
  --username danil \
  --password password \
  --table high_pollution \
  --target-dir /user/danil/water_pollution/high_pollution \
  --as-parquetfile \
  --delete-target-dir \
  --num-mappers 1
