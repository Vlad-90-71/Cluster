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
        var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);

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
                logger.LogInformation(
                    "📥 Получено сообщение из Kafka. Ключ: {Key}", 
                    consumeResult.Message.Key); 

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
                        // 🚀 1. ПОЛУЧАЕМ ТИП ИЗ AssemblyQualifiedName
                        var payloadType = Type.GetType(envelope.PayloadTypeName);

                        if (payloadType == null)
                        {
                            logger.LogError(
                                "Критическая ошибка: не удалось найти тип '{TypeName}' для события '{EventType}'. Проверьте сборку.",
                                envelope.PayloadTypeName, envelope.EventType);

                            // Коммитим, чтобы не зациклить сообщение, которое никогда не обработается из-за отсутствующего типа
                            consumer.Commit(consumeResult);
                            continue;
                        }

                        // 🚀 2. ДЕСЕРИАЛИЗУЕМ JSON В КОНКРЕТНЫЙ ТИП
                        var payloadObj = JsonSerializer.Deserialize(envelope.PayloadJson, payloadType, jsonOptions);
                        
                        if (payloadObj == null)
                        {
                            logger.LogError(
                                "Критическая ошибка: не удалось десериализовать PayloadJson в тип '{TypeName}'. JSON: {Json}",
                                payloadType.FullName, envelope.PayloadJson);

                            // Коммитим, чтобы не зацикливать сообщение с некорректными данными
                            consumer.Commit(consumeResult);
                            continue;
                        }

                        // 🚀 3. ПЕРЕДАЕМ ОБЪЕКТ В ХЕНДЛЕР
                        await handler.HandleAsync(payloadObj, stoppingToken);

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