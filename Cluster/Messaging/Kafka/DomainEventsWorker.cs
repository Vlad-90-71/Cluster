using Cluster.Data;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Cluster.Messaging.Kafka;

// 🚀 DomainEventDispatcher УБРАН из конструктора, чтобы избежать Captive Dependency
public sealed class DomainEventsWorker(
    IServiceScopeFactory scopeFactory,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<DomainEventsWorker> logger) : BackgroundService
{
    private const string DlqTopic = "DomainEvents.DLQ";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafkaOptions.Value;
        var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false, // Отключаем авто-сохранение смещений для ручного контроля
            MaxPollRecords = 100
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(options.Topic);

        logger.LogInformation("🚀 DomainEventsWorker запущен. Подписка на топик: {Topic}; DLQ: {DlqTopic}", options.Topic, DlqTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult.IsPartitionEOF)
                    continue;

                logger.LogDebug("Получено сообщение. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);

                // 1. Десериализация конверта
                DomainEventEnvelope? envelope;
                try
                {
                    envelope = JsonSerializer.Deserialize<DomainEventEnvelope>(consumeResult.Message.Value, jsonOptions);
                }
                catch (JsonException ex)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, null, $"Invalid envelope JSON: {ex.Message}", stoppingToken);
                    continue;
                }

                if (envelope is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, null, "Envelope deserialized to null.", stoppingToken);
                    continue;
                }

                // 2. Десериализация полезной нагрузки (Payload)
                var payloadType = Type.GetType(envelope.PayloadTypeName);
                if (payloadType is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope, $"Payload type not found: {envelope.PayloadTypeName}", stoppingToken);
                    continue;
                }

                object? payload;
                try
                {
                    payload = JsonSerializer.Deserialize(envelope.PayloadJson, payloadType, jsonOptions);
                }
                catch (JsonException ex)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope, $"Payload JSON deserialization failed: {ex.Message}", stoppingToken);
                    continue;
                }

                if (payload is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope, "Payload deserialized to null.", stoppingToken);
                    continue;
                }

                // 3. Обработка события
                // 🚀 СОЗДАЕМ SCOPE ДЛЯ КАЖДОГО СООБЩЕНИЯ. 
                // Это гарантирует, что AppDbContext, DomainEventDispatcher, Registry и сам Handler 
                // будут созданы в одном и том же изолированном контексте и корректно удалены (Dispose) после обработки.
                await using var scope = scopeFactory.CreateAsyncScope();

                var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                // 🚀 ЗАПРАШИВАЕМ ДИСПЕТЧЕР ИЗ ТОГО ЖЕ SCOPE, что и DbContext
                var dispatcher = scope.ServiceProvider.GetRequiredService<DomainEventDispatcher>();

                bool shouldCommit = await dispatcher.DispatchAsync(dbContext, envelope, payload, stoppingToken);

                if (shouldCommit)
                {
                    consumer.Commit(consumeResult);
                }
                else
                {
                    // Dispatch вернул false (ошибка бизнес-логики или БД). 
                    // Отправляем в DLQ и коммитим оригинал, чтобы разорвать бесконечный цикл повторных доставок.
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope, "Processing failed (see dispatcher logs)", stoppingToken);
                }
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Ошибка потребления Kafka.");
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ProduceException<string, string> ex)
            {
                // Если не удалось записать в DLQ, мы НЕ коммитим исходное сообщение. 
                // Kafka доставит его снова при следующем опросе.
                logger.LogError(ex, "Не удалось опубликовать сообщение в DLQ. Исходный offset не будет подтвержден.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Необработанная ошибка в цикле DomainEventsWorker.");
            }
        }
    }

    private async Task SendToDlqAndCommitAsync(
        IConsumer<string, string> consumer,
        IProducer<string, string> producer,
        ConsumeResult<string, string> consumeResult,
        DomainEventEnvelope? envelope,
        string error,
        CancellationToken cancellationToken)
    {
        var dlqMessage = new
        {
            originalTopic = consumeResult.Topic,
            originalPartition = consumeResult.Partition.Value,
            originalOffset = consumeResult.Offset.Value,
            originalKey = consumeResult.Message.Key,
            originalValue = consumeResult.Message.Value,
            messageId = envelope?.MessageId,
            eventType = envelope?.EventType,
            error,
            failedAtUtc = DateTime.UtcNow
        };

        var value = JsonSerializer.Serialize(dlqMessage);

        await producer.ProduceAsync(
            DlqTopic,
            new Message<string, string>
            {
                Key = consumeResult.Message.Key,
                Value = value
            },
            cancellationToken);

        // 🚀 Подтверждаем исходное сообщение ТОЛЬКО после успешной публикации в DLQ
        consumer.Commit(consumeResult);

        logger.LogError(
            "Сообщение отправлено в DLQ. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, MessageId: {MessageId}, Error: {Error}",
            consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, envelope?.MessageId, error);
    }
}