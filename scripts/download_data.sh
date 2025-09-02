#!/bin/bash

DATA_DIR="../data/raw"
mkdir -p $DATA_DIR

MONTHS=("01" "02" "03")
YEAR="2025"

for MONTH in "${MONTHS[@]}"; do
    FILE_NAME="yellow_tripdata_${YEAR}-${MONTH}.parquet"
    FILE_PATH="${DATA_DIR}/${FILE_NAME}"

    if [ -f "$FILE_PATH" ]; then
        echo "$FILE_NAME already exists, skipping download."
    else
        echo "Downloading $FILE_NAME..."
        wget -c "https://d37ci6vzurychx.cloudfront.net/trip-data/$FILE_NAME" -P $DATA_DIR
    fi
done

echo "All files checked/downloaded!"

