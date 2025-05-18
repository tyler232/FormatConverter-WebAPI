using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace FormatConverter.Controllers {
    [ApiController]
    [Route("api/[controller]")]
    public class ConversionController : ControllerBase {
        private readonly IEnumerable<IConversionService> _conversionServices;
        private readonly ILogger<ConversionController> _logger;

        public ConversionController(
            IEnumerable<IConversionService> conversionServices,
            ILogger<ConversionController> logger)
        {
            _conversionServices = conversionServices;
            _logger = logger;
        }

        // GET: api/conversion/formats
        [HttpGet("formats")]
        public IActionResult GetSupportedFormats() {
            var formats = _conversionServices
                .SelectMany(s => s.GetSupportedConversions())
                .Distinct()
                .OrderBy(f => f);
            
            return Ok(formats);
        }

        // POST: api/conversion/convert
        [HttpPost("convert")]
        public async Task<IActionResult> ConvertFile([FromForm] ConversionRequest request) {
            try {
                if (request.File == null || request.File.Length == 0) {
                    return BadRequest("No file uploaded");
                }

                if (string.IsNullOrWhiteSpace(request.TargetFormat)) {
                    return BadRequest("Target format is required");
                }

                var service = _conversionServices.FirstOrDefault(s => 
                    s.GetSupportedConversions().Contains(request.TargetFormat.ToLower()));

                if (service == null) {
                    return BadRequest($"Unsupported conversion format: {request.TargetFormat}");
                }

                var result = await service.ConvertAsync(request);

                if (!result.Success || result.Data == null || result.ContentType == null) {
                    return BadRequest(result.Error ?? "Conversion failed");
                }

                return File(result.Data, result.ContentType, result.FileName);
            } catch (Exception ex) {
                _logger.LogError(ex, "Error during file conversion");
                return StatusCode(500, "An error occurred while processing your request");
            }
        }
    }
}
