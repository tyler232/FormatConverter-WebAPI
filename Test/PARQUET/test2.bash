curl -k https://localhost:5001/api/conversion/convert \
  -F "file=@test.parquet" \
  -F "targetFormat=parquet-to-json" \
  --output output.json
