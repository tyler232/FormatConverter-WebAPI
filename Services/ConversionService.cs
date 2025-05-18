public abstract class ConversionService : IConversionService {
    protected readonly ILogger<ConversionService> _logger;

    public ConversionService(ILogger<ConversionService> logger) {
        _logger = logger;
    }

    public abstract Task<ConversionResponse> ConvertAsync(ConversionRequest request);
    public abstract IEnumerable<string> GetSupportedConversions();

    protected async Task<Stream> GetFileStream(IFormFile file) {
        var memoryStream = new MemoryStream();
        await file.CopyToAsync(memoryStream);
        memoryStream.Position = 0;
        return memoryStream;
    }

    protected string GenerateOutputFileName(string inputName, string targetFormat) {
        var ext = targetFormat.ToLower() switch
        {
            "json" => ".json",
            "csv" => ".csv",
            "parquet" => ".parquet",
            "excel" => ".xlsx",
            _ => ".bin"
        };
        return Path.GetFileNameWithoutExtension(inputName) + ext;
    }
}
