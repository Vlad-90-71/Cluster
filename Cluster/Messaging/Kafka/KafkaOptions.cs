namespace Cluster.Messaging.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; set; } = string.Empty;
    public KafkaTopicsOptions Topics { get; set; } = new();
    public KafkaConsumerGroupsOptions ConsumerGroups { get; set; } = new();
}

public sealed class KafkaTopicsOptions
{
    public string CreateCustomer { get; set; } = string.Empty;
    public string CustomerCreated { get; set; } = string.Empty;
}

public sealed class KafkaConsumerGroupsOptions
{
    public string CustomerCreation { get; set; } = string.Empty;
    public string CustomerCreated { get; set; } = string.Empty;
}