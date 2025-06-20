#!/usr/bin/env bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/water_pollution \
  --username danil \
  --password password \
  --table pdk \
  --target-dir /user/danil/water_pollution/pdk \
  --as-parquetfile \
  --delete-target-dir \
  --num-mappers 1
