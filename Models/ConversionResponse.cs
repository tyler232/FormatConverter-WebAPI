public class ConversionResponse {
    public bool Success { get; set; }
    public byte[]? Data { get; set; }
    public string? FileName { get; set; }
    public string? ContentType { get; set; }
    public string? Error { get; set; }
    public Dictionary<string, object>? Metadata { get; set; }
}
