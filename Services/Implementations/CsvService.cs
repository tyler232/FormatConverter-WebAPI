using CsvHelper;
using ClosedXML.Excel;
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
                        "csv-to-excel"};
    }

    public override async Task<ConversionResponse> ConvertAsync(ConversionRequest request) {
        try {
            return request.TargetFormat?.ToLower() switch {
                "csv-to-json" => await ConvertCsvToJson(request),
                "csv-to-excel" => await ConvertCsvToExcel(request),
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
}
