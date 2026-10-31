using Cluster.Messaging.Kafka;
using System.Text.Json;

namespace Cluster.Messaging.Handlers;

// DTO для полезной нагрузки этого конкретного события
public sealed record CustomerCreatePayload(string Name, string Email);

// Обработчик для создания клиента
public sealed class CustomerCreateHandler(ILogger<CustomerCreateHandler> logger) : IDomainEventHandler
{
    public string SupportedEventType => "CustomerCreate";

    public Task HandleAsync(string payloadJson, CancellationToken ct)
    {
        // Десериализуем только полезную нагрузку
        var data = JsonSerializer.Deserialize<CustomerCreatePayload>(payloadJson);

        logger.LogInformation("Запрос на создание клиента: {Name}.", data?.Name);

        // Здесь может быть отправка Email, SMS и т.д.
        return Task.CompletedTask;
    }
}

