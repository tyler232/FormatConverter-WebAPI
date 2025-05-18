curl -k https://localhost:5001/api/conversion/convert \
  -F "file=@test.json" \
  -F "targetFormat=json-to-parquet" \
  --output output.parquet
