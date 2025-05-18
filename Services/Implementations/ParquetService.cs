using ClosedXML.Excel;
using CsvHelper;
using Parquet;
using Parquet.Data;
using Parquet.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using System.Data;
using System.Text;

public class ParquetService : ConversionService {
    public ParquetService(ILogger<ConversionService> logger) : base(logger) {
        // empty constructor
    }

    public override IEnumerable<string> GetSupportedConversions()
    {
        return new[] {
            "parquet-to-csv",
            "parquet-to-json",
            "parquet-to-excel"
        };
    }

    public override async Task<ConversionResponse> ConvertAsync(ConversionRequest request) {
        try {
            return request.TargetFormat?.ToLower() switch {
                "parquet-to-csv" => await ConvertParquetToCsv(request),
                "parquet-to-json" => await ConvertParquetToJson(request),
                "parquet-to-excel" => await ConvertParquetToExcel(request),
                _ => throw new NotSupportedException($"Conversion {request.TargetFormat} not supported")
            };
        } catch (Exception ex) {
            _logger.LogError(ex, "Parquet conversion failed");
            return new ConversionResponse {
                Success = false,
                Error = ex.Message
            };
        }
    }

    private async Task<ConversionResponse> ConvertParquetToCsv(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var parquetReader = await ParquetReader.CreateAsync(stream);
        
        var dataFields = parquetReader.Schema.GetDataFields();
        var records = new List<Dictionary<string, object?>>();

        using (var rowGroupReader = parquetReader.OpenRowGroupReader(0)) {
            foreach (var field in dataFields) {
                var column = await rowGroupReader.ReadColumnAsync(field);
                for (int i = 0; i < column.Data.Length; i++) {
                    if (i >= records.Count) {
                        records.Add(new Dictionary<string, object?>());
                    }
                    records[i][field.Name] = column.Data.GetValue(i);
                }
            }
        }

        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, leaveOpen: true))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture)) {
            foreach (var field in dataFields) {
                csv.WriteField(field.Name);
            }
            csv.NextRecord();

            foreach (var record in records) {
                foreach (var field in dataFields) {
                    csv.WriteField(record.TryGetValue(field.Name, out var value) ? value?.ToString() : null);
                }
                csv.NextRecord();
            }
            await writer.FlushAsync();
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "csv"),
            ContentType = "text/csv",
            Metadata = new Dictionary<string, object> {
                { "rowCount", records.Count },
                { "columns", dataFields.Select(f => f.Name).ToArray() }
            }
        };
    }

    private async Task<ConversionResponse> ConvertParquetToJson(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var parquetReader = await ParquetReader.CreateAsync(stream);
        
        var dataFields = parquetReader.Schema.GetDataFields();
        var records = new List<Dictionary<string, object?>>();

        using (var rowGroupReader = parquetReader.OpenRowGroupReader(0)) {
            foreach (var field in dataFields) {
                var column = await rowGroupReader.ReadColumnAsync(field);
                for (int i = 0; i < column.Data.Length; i++) {
                    if (i >= records.Count) {
                        records.Add(new Dictionary<string, object?>());
                    }
                    records[i][field.Name] = column.Data.GetValue(i);
                }
            }
        }

        var json = JsonConvert.SerializeObject(records, Formatting.Indented);
        
        return new ConversionResponse {
            Success = true,
            Data = Encoding.UTF8.GetBytes(json),
            FileName = GenerateOutputFileName(request.File.FileName, "json"),
            ContentType = "application/json",
            Metadata = new Dictionary<string, object> {
                { "recordCount", records.Count },
                { "columns", dataFields.Select(f => f.Name).ToArray() }
            }
        };
    }

    private async Task<ConversionResponse> ConvertParquetToExcel(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var parquetReader = await ParquetReader.CreateAsync(stream);
        
        var dataFields = parquetReader.Schema.GetDataFields();
        var records = new List<Dictionary<string, object?>>();

        using (var rowGroupReader = parquetReader.OpenRowGroupReader(0)) {
            foreach (var field in dataFields) {
                var column = await rowGroupReader.ReadColumnAsync(field);
                for (int i = 0; i < column.Data.Length; i++) {
                    if (i >= records.Count) {
                        records.Add(new Dictionary<string, object?>());
                    }
                    records[i][field.Name] = column.Data.GetValue(i);
                }
            }
        }

        using var memoryStream = new MemoryStream();
        using (var workbook = new XLWorkbook()) {
            var worksheet = workbook.Worksheets.Add("Sheet1");
            
            for (int col = 0; col < dataFields.Length; col++) {
                worksheet.Cell(1, col + 1).Value = dataFields[col].Name;
            }

            for (int row = 0; row < records.Count; row++) {
                for (int col = 0; col < dataFields.Length; col++) {
                    var fieldName = dataFields[col].Name;
                    worksheet.Cell(row + 2, col + 1).Value = 
                        records[row].TryGetValue(fieldName, out var value) ? value?.ToString() : null;
                }
            }

            workbook.SaveAs(memoryStream);
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "xlsx"),
            ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Metadata = new Dictionary<string, object> {
                { "rowCount", records.Count },
                { "columns", dataFields.Select(f => f.Name).ToArray() }
            }
        };
    }
}