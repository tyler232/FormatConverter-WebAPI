using System.Collections.Generic;
using System.Threading.Tasks;

public interface IConversionService {
    Task<ConversionResponse> ConvertAsync(ConversionRequest request);
    IEnumerable<string> GetSupportedConversions();
}
