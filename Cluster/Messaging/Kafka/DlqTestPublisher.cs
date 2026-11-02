using System.Text.Json;

namespace Cluster.Messaging.Kafka;

public sealed class DlqTestPublisher(KafkaProducer producer)
{
    public Task PublishTestMessageAsync(CancellationToken cancellationToken = default)
    {
        var message = JsonSerializer.Serialize(new
        {
            originalTopic = "DomainEvents",
            originalPartition = -1,
            originalOffset = -1,
            originalKey = "manual-test",
            originalValue = "not-valid-json",
            messageId = (Guid?)null,
            eventType = "DLQ_TEST",
            error = "Тестовая ошибка обработки",
            failedAtUtc = DateTime.UtcNow
        });

        return producer.PublishAsync(
            "DomainEvents.DLQ",
            Guid.NewGuid().ToString(),
            message,
            cancellationToken);
    }
}