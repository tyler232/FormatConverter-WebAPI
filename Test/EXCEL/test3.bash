curl -k https://localhost:5001/api/conversion/convert \
  -F "file=@test.xlsx" \
  -F "targetFormat=excel-to-parquet" \
  --output output.parquet
