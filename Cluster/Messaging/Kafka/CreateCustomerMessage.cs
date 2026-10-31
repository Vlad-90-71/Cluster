namespace Cluster.Messaging.Kafka;

public sealed record CreateCustomerMessage(Guid MessageId, string Name, string Email, DateTime CreatedAtUtc);