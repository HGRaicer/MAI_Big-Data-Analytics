#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
BASE='https://raw.githubusercontent.com/MAIStudents/BDSnowflake/main/%D0%B8%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5'

files=(
  'MOCK_DATA.csv|MOCK_DATA.csv'
  'MOCK_DATA%20%281%29.csv|MOCK_DATA (1).csv'
  'MOCK_DATA%20%282%29.csv|MOCK_DATA (2).csv'
  'MOCK_DATA%20%283%29.csv|MOCK_DATA (3).csv'
  'MOCK_DATA%20%284%29.csv|MOCK_DATA (4).csv'
  'MOCK_DATA%20%285%29.csv|MOCK_DATA (5).csv'
  'MOCK_DATA%20%286%29.csv|MOCK_DATA (6).csv'
  'MOCK_DATA%20%287%29.csv|MOCK_DATA (7).csv'
  'MOCK_DATA%20%288%29.csv|MOCK_DATA (8).csv'
  'MOCK_DATA%20%289%29.csv|MOCK_DATA (9).csv'
)

for item in "${files[@]}"; do
  remote="${item%%|*}"
  local_name="${item##*|}"
  echo "Downloading ${local_name}"
  curl -fL "${BASE}/${remote}" -o "data/${local_name}"
done

echo "All CSV files are in ./data"
