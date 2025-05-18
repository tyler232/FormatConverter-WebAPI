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

public class CsvService : ConversionService {
    public CsvService(ILogger<ConversionService> logger) : base(logger) {
        // empty constructor
    }

    public override IEnumerable<string> GetSupportedConversions() {
        return new[] { "csv-to-json",
                        "csv-to-excel",
                        "csv-to-parquet"};
    }

    public override async Task<ConversionResponse> ConvertAsync(ConversionRequest request) {
        try {
            return request.TargetFormat?.ToLower() switch {
                "csv-to-json" => await ConvertCsvToJson(request),
                "csv-to-excel" => await ConvertCsvToExcel(request),
                "csv-to-parquet" => await ConvertCsvToParquet(request),
                _ => throw new NotSupportedException($"Conversion {request.TargetFormat} not supported")
            };
        } catch (Exception ex) {
            _logger.LogError(ex, "CSV conversion failed");
            return new ConversionResponse { 
                Success = false, 
                Error = ex.Message 
            };
        }
    }

    private async Task<ConversionResponse> ConvertCsvToJson(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File), "File cannot be null");
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        var records = csv.GetRecords<dynamic>().ToList();
        var json = JsonConvert.SerializeObject(records, Formatting.Indented);
        
        return new ConversionResponse {
            Success = true,
            Data = Encoding.UTF8.GetBytes(json),
            FileName = GenerateOutputFileName(request.File.FileName, "json"),
            ContentType = "application/json",
            Metadata = new Dictionary<string, object> { { "recordCount", records.Count } }
        };
    }

    private async Task<ConversionResponse> ConvertCsvToExcel(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        
        using var dataTable = new DataTable();
        using var dataReader = new CsvDataReader(csv);
        dataTable.Load(dataReader);

        using var memoryStream = new MemoryStream();
        using (var workbook = new XLWorkbook()) {
            var worksheet = workbook.Worksheets.Add(dataTable, "Sheet1");
            workbook.SaveAs(memoryStream);
        }

        return new ConversionResponse {
            Success = true,
            Data = memoryStream.ToArray(),
            FileName = GenerateOutputFileName(request.File.FileName, "xlsx"),
            ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Metadata = new Dictionary<string, object> { { "rowCount", dataTable.Rows.Count } }
        };
    }

    private async Task<ConversionResponse> ConvertCsvToParquet(ConversionRequest request) {
        if (request.File == null) {
            throw new ArgumentNullException(nameof(request.File));
        }

        await using var stream = await GetFileStream(request.File);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        
        await csv.ReadAsync();
        csv.ReadHeader();

        var headers = csv.HeaderRecord ?? throw new InvalidDataException("No headers found in CSV file");
        var schema = new ParquetSchema(
            headers.Select(h => new DataField<string>(h)).ToArray()
        );

        var records = new List<Dictionary<string, string>>();
        while (await csv.ReadAsync()) {
            var record = new Dictionary<string, string>();
            foreach (var header in headers) {
                var value = csv.GetField(header);
                record[header] = value ?? string.Empty;
            }
            records.Add(record);
        }

        using var memoryStream = new MemoryStream();
        using (var parquetWriter = await ParquetWriter.CreateAsync(schema, memoryStream)) {
            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
                foreach (var header in headers) {
                    var columnData = records.Select(r => r[header]).ToArray();
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
                { "rowCount", records.Count },
                { "columns", headers }
            }
        };
    }
}
