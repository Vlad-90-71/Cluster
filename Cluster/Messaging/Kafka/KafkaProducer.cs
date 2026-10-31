using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Cluster.Messaging.Kafka;

public sealed class KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger) : IDisposable
{
    private readonly IProducer<string, string> _producer =
        new ProducerBuilder<string, string>(
            new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true
            }).Build();

    public async Task PublishAsync(string topic, string key, string message, CancellationToken cancellationToken = default)
    {
        var result = await _producer.ProduceAsync(
            topic,
            new Message<string, string>
            {
                Key = key,
                Value = message
            },
            cancellationToken);

        logger.LogInformation(
            "Kafka message published. Topic={Topic}, Partition={Partition}, Offset={Offset}",
            result.Topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}