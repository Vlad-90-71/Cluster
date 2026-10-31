namespace Cluster.Messaging.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;
    public string Topic { get; set; } = "DomainEvents";
    public string ConsumerGroup { get; set; } = "domain-events-group";
}
