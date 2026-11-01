using Cluster.Data;
using Cluster.Messaging.Handlers;
using Cluster.Messaging.Kafka;
using Confluent.Kafka;
using Medallion.Threading;
using Medallion.Threading.Redis;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
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

builder.Services.AddSingleton<IDistributedLockProvider>(sp =>
{
    // Берем уже созданный и подключенный к Sentinel экземпляр redis из DI
    var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();

    // Используем базу данных по умолчанию (обычно 0)
    var database = connectionMultiplexer.GetDatabase();

    return new RedisDistributedSynchronizationProvider(database);
});

// Общие ключи Data Protection для обеих реплик приложения.
builder.Services.AddDataProtection()
    .SetApplicationName("Cluster.Mvc")
    .PersistKeysToStackExchangeRedis(redis, "DataProtection-Keys");

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(x => !string.IsNullOrWhiteSpace(x.BootstrapServers), "Kafka BootstrapServers is required.")
    .ValidateOnStart();

builder.Services.AddSingleton<KafkaProducer>();

// 1. Регистрируем Реестр обработчиков
builder.Services.AddSingleton<DomainEventHandlerRegistry>();

// 2. Автоматически регистрируем ВСЕ классы, реализующие IDomainEventHandler
// (Если не используете Scrutor, можно добавить их вручную по одному)
builder.Services.Scan(scan => scan
    .FromAssemblyOf<Program>()
    .AddClasses(classes => classes.AssignableTo<IDomainEventHandler>())
    .AsImplementedInterfaces()
    .WithSingletonLifetime());

// 2. Регистрируем обработчики вручную
//builder.Services.AddSingleton<CustomerCreatedHandler>();
// В будущем добавляйте новые обработчики здесь:
// builder.Services.AddSingleton<AnotherEventHandler>();

// 3. Регистрируем ЕДИНЫЙ фоновый воркер
builder.Services.AddHostedService<DomainEventsWorker>();

// Регистрируем фоновый процесс доставки событий из БД в Kafka
builder.Services.AddHostedService<DomainEventsPublisherWorker>();

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

// ИНИЦИАЛИЗАЦИЯ СТАТИЧЕСКОГО КЛАССА
// Извлекаем провайдер из DI контейнера и передаем его в наш executor
var lockProvider = app.Services.GetRequiredService<IDistributedLockProvider>();
DbTransactionExecutor.Initialize(lockProvider);

await EnsureKafkaTopicExistsAsync(app.Services);

app.Run();

static async Task EnsureKafkaTopicExistsAsync(IServiceProvider services)
{
    var options = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
    using var adminClient = new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = options.BootstrapServers
    }).Build();

    try
    {
        await adminClient.CreateTopicsAsync(
        [
            new Confluent.Kafka.Admin.TopicSpecification
            {
                Name = options.Topic,
                NumPartitions = 4, // Оптимально для 2 узлов (можно масштабировать до 4-8 воркеров)
                ReplicationFactor = 2
            }
        ]);
        Console.WriteLine($"Топик '{options.Topic}' создан или уже существует.");
    }
    catch (Confluent.Kafka.Admin.CreateTopicsException e)
    {
        if (!e.Results.All(r => r.Error.Code == Confluent.Kafka.ErrorCode.TopicAlreadyExists))
            throw;
    }
}