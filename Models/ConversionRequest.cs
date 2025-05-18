public class ConversionRequest {
    public IFormFile? File { get; set; }
    public string? TargetFormat { get; set; }
    public Dictionary<string, string>? Options { get; set; }
}
