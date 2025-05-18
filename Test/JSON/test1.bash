curl -k https://localhost:5001/api/conversion/convert \
  -F "file=@test.json" \
  -F "targetFormat=json-to-csv" \
  --output output.csv
  
