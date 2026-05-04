#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
BASE='https://raw.githubusercontent.com/MAIStudents/BigDataSpark/main/%D0%B8%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5'

curl -fL "${BASE}/MOCK_DATA.csv" -o "data/MOCK_DATA.csv"
for i in {1..9}; do
  remote="MOCK_DATA%20%28${i}%29.csv"
  local_name="MOCK_DATA (${i}).csv"
  echo "Downloading ${local_name}"
  curl -fL "${BASE}/${remote}" -o "data/${local_name}"
done

echo "Downloaded CSV files:"
ls -1 data/*.csv
