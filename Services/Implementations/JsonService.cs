using CsvHelper;
using ClosedXML.Excel;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using System.Data;
using System.Text;

public class JsonService : ConversionService {
    public JsonService(ILogger<ConversionService> logger) : base(logger) {
        // empty constructor
    }

    public override IEnumerable<string> GetSupportedConversions()
    {
        return new[] {
            "json-to-csv",
            "json-to-excel",
            "json-to-parquet"
        };
    }

    public override async Task<ConversionResponse> ConvertAsync(ConversionRequest request) {
        try {
            return request.TargetFormat?.ToLower() switch {
                "json-to-csv" => await ConvertJsonToCsv(request),
                "json-to-excel" => await ConvertJsonToExcel(request),
                "json-to-parquet" => await ConvertJsonToParquet(request),
                _ => throw new NotSupportedException($"Conversion {request.TargetFormat} not supported")
            };
        } catch (Exception ex) {
            _logger.LogError(ex, "JSON conversion failed");
            return new ConversionResponse {
                Success = false,
                Error = ex.Message
            };
        }
    }

    private async Task<ConversionResponse> ConvertJsonToCsv(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File), "File cannot be null");
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        var json = await reader.ReadToEndAsync();
        var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(json) 
            ?? throw new InvalidDataException("Invalid JSON format");

        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, leaveOpen: true))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture)) {
            if (data.Count > 0) {
                foreach (var key in data[0].Keys) {
                    csv.WriteField(key);
                }
                csv.NextRecord();

                foreach (var record in data) {
                    foreach (var value in record.Values) {
                        csv.WriteField(value?.ToString());
                    }
                    csv.NextRecord();
                }
            }
            await writer.FlushAsync();
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "csv"),
            ContentType = "text/csv",
            Metadata = new Dictionary<string, object> { { "recordCount", data.Count } }
        };
    }

    private async Task<ConversionResponse> ConvertJsonToExcel(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        var json = await reader.ReadToEndAsync();
        var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(json)
            ?? throw new InvalidDataException("Invalid JSON format");

        using var memoryStream = new MemoryStream();
        using (var workbook = new XLWorkbook()) {
            var worksheet = workbook.Worksheets.Add("Sheet1");
            
            if (data.Count > 0) {
                var headers = data[0].Keys.ToList();
                for (int i = 0; i < headers.Count; i++) {
                    worksheet.Cell(1, i + 1).Value = headers[i];
                }

                for (int row = 0; row < data.Count; row++) {
                    var record = data[row];
                    for (int col = 0; col < headers.Count; col++) {
                        worksheet.Cell(row + 2, col + 1).Value = record[headers[col]]?.ToString();
                    }
                }
            }
            workbook.SaveAs(memoryStream);
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "xlsx"),
            ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Metadata = new Dictionary<string, object> { { "rowCount", data.Count } }
        };
    }

    private async Task<ConversionResponse> ConvertJsonToParquet(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        var json = await reader.ReadToEndAsync();
        var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(json)
            ?? throw new InvalidDataException("Invalid JSON format");

        if (data.Count == 0) {
            return new ConversionResponse {
                Success = true,
                Data = Array.Empty<byte>(),
                FileName = GenerateOutputFileName(request.File.FileName, "parquet"),
                ContentType = "application/octet-stream",
                Metadata = new Dictionary<string, object> { { "rowCount", 0 } }
            };
        }

        var headers = data[0].Keys.ToList();
        var schema = new ParquetSchema(
            headers.Select(h => new DataField<string>(h)).ToArray()
        );

        using var memoryStream = new MemoryStream();
        using (var parquetWriter = await ParquetWriter.CreateAsync(schema, memoryStream)) {
            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
                foreach (var header in headers) {
                    var columnData = data.Select(r => r[header]?.ToString() ?? string.Empty).ToArray();
                    await groupWriter.WriteColumnAsync(new Parquet.Data.DataColumn(
                        schema.DataFields.First(f => f.Name == header),
                        columnData));
                }
            }
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "parquet"),
            ContentType = "application/octet-stream",
            Metadata = new Dictionary<string, object> {
                { "rowCount", data.Count },
                { "columns", headers }
            }
        };
    }
}