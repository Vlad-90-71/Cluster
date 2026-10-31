using Cluster.Messaging.Kafka;
using System.Text.Json;

namespace Cluster.Messaging.Handlers;

// DTO для полезной нагрузки этого конкретного события
public sealed record CustomerCreatedPayload(long CustomerId, string Name, string Email);

// Обработчик для создания клиента
public sealed class CustomerCreatedHandler(ILogger<CustomerCreatedHandler> logger) : IDomainEventHandler
{
    public string SupportedEventType => "CustomerCreated";

    public Task HandleAsync(string payloadJson, CancellationToken ct)
    {
        // Десериализуем только полезную нагрузку
        var data = JsonSerializer.Deserialize<CustomerCreatedPayload>(payloadJson);

        logger.LogInformation("[CustomerCreated] Успешно создан клиент: {Name} (ID: {Id})", data?.Name, data?.CustomerId);

        // Здесь может быть отправка Email, SMS и т.д.
        return Task.CompletedTask;
    }
}

