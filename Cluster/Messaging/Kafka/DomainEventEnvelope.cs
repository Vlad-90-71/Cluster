using System.Text.Json.Serialization;

namespace Cluster.Messaging.Kafka;

public sealed record DomainEventEnvelope
{
    [JsonPropertyName("eventType")]
    public string EventType { get; set; } = string.Empty; // Например: "CustomerCreated"

    [JsonPropertyName("messageId")]
    public Guid MessageId { get; set; } // Для идемпотентности

    [JsonPropertyName("payload")]
    public string PayloadJson { get; set; } = string.Empty; // JSON с данными события

    [JsonPropertyName("occurredAtUtc")]
    public DateTime OccurredAtUtc { get; set; } = DateTime.UtcNow;
}