using Cluster.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Cluster.Messaging.Kafka;

public sealed class DomainEventsPublisherWorker(
    IServiceProvider serviceProvider,
    KafkaProducer kafkaProducer,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<DomainEventsPublisherWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("🚀 DomainEventsPublisherWorker запущен. Ожидание событий...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                // Ищем неподтвержденные сообщения. 
                var unpublishedMessages = await dbContext.TransactionOperations
                    .AsNoTracking()
                    .Where(x => x.MessageType != null && x.ResultJson != null && !x.IsPublished)
                    .OrderBy(x => x.CreatedAtUtc)
                    .Take(50)
                    .Select(x => new
                    {
                        x.Id,
                        x.IdempotencyKey,
                        MessageType = x.MessageType!,
                        ResultJson = x.ResultJson!,
                        x.CreatedAtUtc
                    })
                    .ToListAsync(stoppingToken);

                if (unpublishedMessages.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                    continue;
                }

                logger.LogInformation("Найдено {Count} неподтвержденных событий. Начинаем отправку...", unpublishedMessages.Count);

                var successfullyPublishedIds = new List<long>();

                foreach (var message in unpublishedMessages)
                {
                    try
                    {
                        // 🚀 СОБИРАЕМ КОНВЕРТ "НА ЛЕТУ" из полей БД
                        var envelope = new DomainEventEnvelope
                        {
                            EventType = message.MessageType!,
                            MessageId = message.IdempotencyKey,
                            PayloadJson = message.ResultJson!, 
                            OccurredAtUtc = message.CreatedAtUtc
                        };

                        await kafkaProducer.PublishAsync(
                            topic: kafkaOptions.Value.Topic,
                            key: message.IdempotencyKey.ToString(),
                            message: JsonSerializer.Serialize(envelope),
                            cancellationToken: stoppingToken);

                        successfullyPublishedIds.Add(message.Id);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Ошибка публикации сообщения {IdempotencyKey} (Тип: {MessageType}). Будет повторная попытка.",
                            message.IdempotencyKey, message.MessageType);
                    }
                }

                // 2. БЫСТРОЕ ОБНОВЛЕНИЕ БЕЗ ТРЕКИНГА
                if (successfullyPublishedIds.Count > 0)
                {
                    await dbContext.TransactionOperations
                        .Where(x => successfullyPublishedIds.Contains(x.Id))
                        .ExecuteUpdateAsync(setters => setters.SetProperty(x => x.IsPublished, true), stoppingToken);

                    logger.LogInformation("Успешно обновлен статус для {Count} событий.", successfullyPublishedIds.Count);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Критическая ошибка в цикле DomainEventsPublisherWorker. Повтор через 5 секунд...");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}