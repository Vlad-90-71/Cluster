using Cluster.Application.Customers.Commands;
using Cluster.Messaging.Kafka;

namespace Cluster.Messaging.Handlers;

public sealed class CustomerCreatedHandler(ILogger<CustomerCreatedHandler> logger) : IDomainEventHandler
{
    public string SupportedEventType => "CustomerCreated";

    public Task HandleAsync(object payload, CancellationToken ct)
    {
        // 🚀 БЕЗОПАСНОЕ ПРИВЕДЕНИЕ ТИПА
        if (payload is CustomerCreatedData data)
        {
            logger.LogInformation(
                "🎉 [СОБЫТИЕ] Клиент успешно создан! ID: {Id}, Имя: {Name}, Email: {Email}",
                data.Id, data.Name, data.Email);

            // Здесь можно вызывать email-сервисы, отправлять вебхуки и т.д.
            // Все данные уже строго типизированы, никаких GetProperty("id")!
        }
        else
        {
            // Защита от дурака: если по какой-то причине пришел не тот тип
            logger.LogWarning(
                "⚠️ Неожиданный тип полезной нагрузки для CustomerCreated. Ожидался CustomerCreatedData, получен: {Type}",
                payload?.GetType().FullName);
        }

        return Task.CompletedTask;
    }
}
