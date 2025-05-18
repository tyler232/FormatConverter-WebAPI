using System.Security.Authentication;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(serverOptions => {
    serverOptions.ConfigureHttpsDefaults(httpsOptions => {
        httpsOptions.SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;
        httpsOptions.ClientCertificateMode = ClientCertificateMode.AllowCertificate;
    });
    
    // listen on port 5000 for http and 5001 for https
    serverOptions.ListenLocalhost(5000);
    serverOptions.ListenLocalhost(5001, listenOptions => {
        listenOptions.UseHttps();
    });
});

builder.Services.AddTransient<IConversionService, CsvService>();
builder.Services.AddTransient<IConversionService, JsonService>();
builder.Services.AddTransient<IConversionService, ExcelService>();
builder.Services.AddControllers().AddNewtonsoftJson();

builder.Services.Configure<KestrelServerOptions>(options =>
{
    options.Limits.MaxRequestBodySize = 1073741824; // 1GB
});
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = 1073741824; // 1GB
});

var app = builder.Build();

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
