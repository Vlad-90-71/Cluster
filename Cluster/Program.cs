using Cluster.Data;
using Cluster.Messaging.Kafka;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        npgsql => npgsql.EnableRetryOnFailure(
            maxRetryCount: 5,
            maxRetryDelay: TimeSpan.FromSeconds(5),
            errorCodesToAdd: null)));

builder.Services.AddDbContext<ReadDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("ReadConnection"),
        npgsql => npgsql.EnableRetryOnFailure(
            maxRetryCount: 5,
            maxRetryDelay: TimeSpan.FromSeconds(5),
            errorCodesToAdd: null)));

var redisSection = builder.Configuration.GetSection("Redis");
var sentinelSection = redisSection.GetSection("Sentinel");

var serviceName = sentinelSection["ServiceName"]
    ?? throw new InvalidOperationException("Redis:Sentinel:ServiceName is not configured.");

var sentinelEndpoints = sentinelSection.GetSection("Endpoints").Get<string[]>()
    ?? throw new InvalidOperationException("Redis:Sentinel:Endpoints is not configured.");

var redisPassword = redisSection["Password"]
    ?? throw new InvalidOperationException("Redis:Password is not configured.");

var sentinelOptions = new ConfigurationOptions
{
    AbortOnConnectFail = false,
    ConnectTimeout = 5000
};

foreach (var endpoint in sentinelEndpoints)
{
    var parts = endpoint.Split(':', 2);

    if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        throw new InvalidOperationException($"Invalid Redis Sentinel endpoint: {endpoint}");

    sentinelOptions.EndPoints.Add(parts[0], port);
}

var sentinel = ConnectionMultiplexer.SentinelConnect(sentinelOptions);

var redisOptions = new ConfigurationOptions
{
    ServiceName = serviceName,
    Password = redisPassword,
    AbortOnConnectFail = false,
    ConnectTimeout = 5000
};

var redis = sentinel.GetSentinelMasterConnection(redisOptions);

builder.Services.AddSingleton<IConnectionMultiplexer>(redis);
builder.Services.AddSingleton(sentinel);

builder.Services.AddStackExchangeRedisCache(options =>
{
    options.ConnectionMultiplexerFactory = () =>
        Task.FromResult<IConnectionMultiplexer>(redis);

    options.InstanceName = "Cluster:";
});

// Общие ключи Data Protection для обеих реплик приложения.
builder.Services.AddDataProtection()
    .SetApplicationName("Cluster.Mvc")
    .PersistKeysToStackExchangeRedis(redis, "DataProtection-Keys");

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(x => !string.IsNullOrWhiteSpace(x.BootstrapServers),
        "Kafka BootstrapServers is required.")
    .ValidateOnStart();

builder.Services.AddSingleton<KafkaProducer>();

builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(Program).Assembly));

// Add services to the container.
builder.Services.AddControllersWithViews();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();

    app.UseHttpsRedirection();
}

app.UseRouting();

app.UseAuthorization();

app.MapStaticAssets();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}")
    .WithStaticAssets();


app.Run();
