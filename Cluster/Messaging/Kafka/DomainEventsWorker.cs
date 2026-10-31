using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Cluster.Messaging.Kafka;

public sealed class DomainEventsWorker(
    IOptions<KafkaOptions> kafkaOptions,
    DomainEventHandlerRegistry eventRegistry,
    ILogger<DomainEventsWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafkaOptions.Value;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            MaxPollRecords = 100
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(options.Topic);

        logger.LogInformation(" DomainEventsWorker запущен. Подписка на топик: {Topic}", options.Topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                var envelope = JsonSerializer.Deserialize<DomainEventEnvelope>(consumeResult.Message.Value);

                if (envelope == null)
                {
                    logger.LogWarning("Не удалось десериализовать сообщение. Пропуск.");
                    consumer.Commit(consumeResult);
                    continue;
                }

                // 🚀 МАРШРУТИЗАЦИЯ: Находим нужный обработчик за O(1)
                if (eventRegistry.TryGetHandler(envelope.EventType, out var handler) && handler != null)
                {
                    try
                    {
                        // Выполняем бизнес-логику конкретного события
                        await handler.HandleAsync(envelope.PayloadJson, stoppingToken);

                        // Коммитим ТОЛЬКО после успешной обработки
                        consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Ошибка в обработчике события {EventType}. Сообщение будет обработано повторно.", envelope.EventType);
                        // НЕ делаем коммит, чтобы сообщение вернулось в очередь при следующем poll()
                    }
                }
                else
                {
                    logger.LogWarning("Не найден обработчик для EventType: {EventType}. Пропускаем и коммитим.", envelope.EventType);
                    consumer.Commit(consumeResult); // Пропускаем неизвестные события, чтобы не зациклиться
                }
            }
            catch (ConsumeException e)
            {
                logger.LogError(e, "Ошибка потребления Kafka");
            }
            catch (OperationCanceledException)
            {
                // Корректное завершение работы
                break;
            }
        }
    }
}