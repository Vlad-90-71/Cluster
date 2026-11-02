using Cluster.Data;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Cluster.Messaging.Kafka;

public sealed class DomainEventsWorker(
    IServiceScopeFactory scopeFactory,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<DomainEventsWorker> logger) : BackgroundService
{
    private const int MaxAttempts = 5;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);
    private const string DlqTopic = "DomainEvents.DLQ";
    private const bool SimulateProcessingFailure = true;
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
            EnableAutoOffsetStore = false,
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

        logger.LogInformation(
            "DomainEventsWorker запущен. Подписка на топик: {Topic}; DLQ: {DlqTopic}",
            options.Topic, DlqTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult.IsPartitionEOF)
                    continue;

                logger.LogInformation(
                    "Получено сообщение из Kafka. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}",
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, consumeResult.Message.Key);

                DomainEventEnvelope? envelope;

                try
                {
                    envelope = JsonSerializer.Deserialize<DomainEventEnvelope>(
                        consumeResult.Message.Value, jsonOptions);
                }
                catch (JsonException ex)
                {
                    await SendToDlqAndCommitAsync(consumer,  dlqProducer, consumeResult, null,
                        $"Invalid envelope JSON: {ex.Message}", stoppingToken);

                    continue;
                }

                if (envelope is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, null,
                        "Envelope deserialized to null.", stoppingToken);

                    continue;
                }

                var payloadType = Type.GetType(envelope.PayloadTypeName);

                if (payloadType is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope,
                        $"Payload type not found: {envelope.PayloadTypeName}", stoppingToken);

                    continue;
                }

                object? payload;

                try
                {
                    payload = JsonSerializer.Deserialize(envelope.PayloadJson, payloadType, jsonOptions);
                }
                catch (JsonException ex)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope,
                        $"Payload JSON deserialization failed: {ex.Message}", stoppingToken);

                    continue;
                }

                if (payload is null)
                {
                    await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope,
                        "Payload deserialized to null.", stoppingToken);

                    continue;
                }

                var processed = false;
                var attempts = 0;
                string? lastError = null;

                await using var scope = scopeFactory.CreateAsyncScope();

                var services = scope.ServiceProvider;

                var eventRegistry = services.GetRequiredService<DomainEventHandlerRegistry>();

                if (eventRegistry.TryGetHandler(envelope.EventType, out var handler) && handler is not null)
                {
                    var dbContext = services.GetRequiredService<AppDbContext>();

                    while (!stoppingToken.IsCancellationRequested && attempts < MaxAttempts)
                    {
                        attempts++;

                        try
                        {
                            var result = await DbTransactionExecutor.UpdateAsync(
                                dbContext,
                                envelope.MessageId,
                                (_, ct) => handler.HandleAsync(payload, ct),
                                logger,
                                stoppingToken);

                            if (result.IsSuccess)
                            {
                                processed = true;

                                logger.LogInformation(
                                    "Событие {MessageId} обработано. Attempt: {Attempt}/{MaxAttempts}.",
                                    envelope.MessageId, attempts, MaxAttempts);

                                break;
                            }

                            lastError = $"{result.FailureReason}: {result.ErrorMessage}";

                            logger.LogWarning(
                                "Попытка {Attempt}/{MaxAttempts} обработки события {MessageId} не удалась. {Error}",
                                attempts, MaxAttempts, envelope.MessageId, lastError);
                        }
                        catch (OperationCanceledException)
                            when (stoppingToken.IsCancellationRequested)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            lastError = ex.Message;

                            logger.LogError(ex,
                                "Ошибка попытки {Attempt}/{MaxAttempts} обработки события {MessageId}.",
                                attempts, MaxAttempts, envelope.MessageId);
                        }

                        if (attempts < MaxAttempts)
                            await Task.Delay(RetryDelay, stoppingToken);
                    }
                }
                else
                    lastError = $"Handler not found for event type '{envelope.EventType}'.";

                if (processed)
                {
                    consumer.Commit(consumeResult);
                    continue;
                }

                await SendToDlqAndCommitAsync(consumer, dlqProducer, consumeResult, envelope,
                    lastError ?? "Unknown processing error.", stoppingToken);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Ошибка потребления Kafka.");
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ProduceException<string, string> ex)
            {
                // Не подтверждаем исходный offset, если публикация в DLQ не удалась.
                logger.LogError(ex, "Не удалось опубликовать сообщение в DLQ. Исходный offset не подтверждён.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Необработанная ошибка DomainEventsWorker.");
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

        // Подтверждаем исходное сообщение только после успешной публикации в DLQ.
        consumer.Commit(consumeResult);

        logger.LogError(
            "Сообщение отправлено в DLQ. Original topic: {Topic}, Partition: {Partition}, Offset: {Offset}, MessageId: {MessageId}, Error: {Error}",
            consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, envelope?.MessageId, error);
    }
}