curl -k https://localhost:5001/api/conversion/convert \
  -F "file=@test.csv" \
  -F "targetFormat=csv-to-json" \
  --output output.json
  
