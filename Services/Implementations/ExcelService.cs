using ClosedXML.Excel;
using CsvHelper;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using System.Data;
using System.Text;

public class ExcelService : ConversionService {
    public ExcelService(ILogger<ConversionService> logger) : base(logger) {
        // empty constructor
    }

    public override IEnumerable<string> GetSupportedConversions() {
        return new[] {
            "excel-to-csv",
            "excel-to-json",
            "excel-to-parquet"
        };
    }

    public override async Task<ConversionResponse> ConvertAsync(ConversionRequest request) {
        try {
            return request.TargetFormat?.ToLower() switch {
                "excel-to-csv" => await ConvertExcelToCsv(request),
                "excel-to-json" => await ConvertExcelToJson(request),
                "excel-to-parquet" => await ConvertExcelToParquet(request),
                _ => throw new NotSupportedException($"Conversion {request.TargetFormat} not supported")
            };
        } catch (Exception ex) {
            _logger.LogError(ex, "Excel conversion failed");
            return new ConversionResponse {
                Success = false,
                Error = ex.Message
            };
        }
    }

    private async Task<ConversionResponse> ConvertExcelToCsv(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File), "File cannot be null");
        }

        await using var stream = await GetFileStream(request.File);
        using var workbook = new XLWorkbook(stream);
        var worksheet = workbook.Worksheet(1); // get first sheet

        var rows = worksheet.RowsUsed();
        if (!rows.Any()) {
            return new ConversionResponse {
                Success = true,
                Data = Array.Empty<byte>(),
                FileName = GenerateOutputFileName(request.File.FileName, "csv"),
                ContentType = "text/csv",
                Metadata = new Dictionary<string, object> { { "rowCount", 0 } }
            };
        }

        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, leaveOpen: true))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture)) {
            foreach (var cell in rows.First().Cells()) {
                csv.WriteField(cell.Value.ToString());
            }
            csv.NextRecord();

            foreach (var row in rows.Skip(1)) {
                foreach (var cell in row.Cells()) {
                    csv.WriteField(cell.Value.ToString());
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
                { "rowCount", rows.Count() - 1 }, // exclude header
                { "columns", rows.First().Cells().Select(c => c.Value.ToString()).ToArray() }
            }
        };
    }

    private async Task<ConversionResponse> ConvertExcelToJson(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var workbook = new XLWorkbook(stream);
        var worksheet = workbook.Worksheet(1);

        var rows = worksheet.RowsUsed();
        if (!rows.Any()) {
            return new ConversionResponse {
                Success = true,
                Data = Encoding.UTF8.GetBytes("[]"),
                FileName = GenerateOutputFileName(request.File.FileName, "json"),
                ContentType = "application/json",
                Metadata = new Dictionary<string, object> { { "rowCount", 0 } }
            };
        }

        var headers = rows.First().Cells().Select(c => c.Value.ToString()).ToArray();
        var records = new List<Dictionary<string, object>>();

        foreach (var row in rows.Skip(1)) {
            var record = new Dictionary<string, object>();
            var cells = row.Cells().ToArray();
            
            for (int i = 0; i < headers.Length; i++) {
                if (i < cells.Length) {
                    var cellValue = cells[i].Value;
                    record[headers[i]] = cellValue.Type switch {
                        XLDataType.Text => cellValue.GetText(),
                        XLDataType.Number => cellValue.GetNumber(),
                        XLDataType.Boolean => cellValue.GetBoolean(),
                        XLDataType.DateTime => cellValue.GetDateTime(),
                        XLDataType.TimeSpan => cellValue.GetTimeSpan(),
                        _ => cellValue.ToString()
                    };
                } else {
                    record[headers[i]] = String.Empty;
                }
            }
            records.Add(record);
        }

        var json = JsonConvert.SerializeObject(records, Formatting.Indented);
        
        return new ConversionResponse {
            Success = true,
            Data = Encoding.UTF8.GetBytes(json),
            FileName = GenerateOutputFileName(request.File.FileName, "json"),
            ContentType = "application/json",
            Metadata = new Dictionary<string, object> { 
                { "recordCount", records.Count },
                { "columns", headers }
            }
        };
    }

    private async Task<ConversionResponse> ConvertExcelToParquet(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var workbook = new XLWorkbook(stream);
        var worksheet = workbook.Worksheet(1);

        var rows = worksheet.RowsUsed();
        if (!rows.Any()) {
            return new ConversionResponse {
                Success = true,
                Data = Array.Empty<byte>(),
                FileName = GenerateOutputFileName(request.File.FileName, "parquet"),
                ContentType = "application/octet-stream",
                Metadata = new Dictionary<string, object> { { "rowCount", 0 } }
            };
        }

        var headers = rows.First().Cells().Select(c => c.Value.ToString()).ToArray();
        var schema = new ParquetSchema(
            headers.Select(h => new DataField<string>(h)).ToArray()
        );

        using var memoryStream = new MemoryStream();
        using (var parquetWriter = await ParquetWriter.CreateAsync(schema, memoryStream)) {
            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
                var dataRows = rows.Skip(1).ToList();
                for (int colIndex = 0; colIndex < headers.Length; colIndex++) {
                    var columnData = new string[dataRows.Count];
                    
                    for (int rowIndex = 0; rowIndex < dataRows.Count; rowIndex++) {
                        var cell = dataRows[rowIndex].Cell(colIndex + 1);
                        columnData[rowIndex] = cell.Value.ToString() ?? string.Empty;
                    }

                    await groupWriter.WriteColumnAsync(new Parquet.Data.DataColumn(
                        schema.DataFields[colIndex],
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
                { "rowCount", rows.Count() - 1 },
                { "columns", headers }
            }
        };
    }
}

