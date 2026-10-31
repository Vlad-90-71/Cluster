using Cluster.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Cluster.Messaging.Kafka;

/// <summary>
/// Фоновый сервис, который забирает неподтвержденные события из таблицы TransactionOperation (Outbox)
/// и гарантирует их доставку в Kafka.
/// </summary>
public sealed class DomainEventsPublisherWorker(
    IServiceProvider serviceProvider,
    KafkaProducer kafkaProducer,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<DomainEventsPublisherWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("🚀 KafkaOutboxPublisherWorker запущен. Ожидание событий для отправки...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Создаем скоуп, чтобы получить свежий DbContext (EF Core контексты не потокобезопасны и не должны жить вечно)
                using var scope = serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                // Ищем пакет неподтвержденных сообщений. 
                // MessageType != null гарантирует, что мы берем только те записи, где требуется отправка в Kafka.
                var unpublishedMessages = await dbContext.TransactionOperations
                    .Where(x => x.MessageType != null && x.MessagePayloadJson != null && !x.IsPublished)
                    .OrderBy(x => x.CreatedAtUtc) // Отправляем строго в порядке создания (FIFO)
                    .Take(50)                     // Обрабатываем пакетами по 50 штук, чтобы не перегружать память
                    .ToListAsync(stoppingToken);

                if (unpublishedMessages.Count == 0)
                {
                    // Если очередь пуста, делаем паузу, чтобы не нагружать БД постоянными запросами
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                    continue;
                }

                logger.LogInformation("Найдено {Count} неподтвержденных событий. Начинаем отправку...", unpublishedMessages.Count);

                foreach (var message in unpublishedMessages)
                {
                    try
                    {
                        // Отправляем готовый JSON напрямую в Kafka
                        await kafkaProducer.PublishAsync(
                            topic: kafkaOptions.Value.Topic, // Единый топик "DomainEvents"
                            key: message.IdempotencyKey.ToString(), // Key гарантирует порядок и идемпотентность
                            message: message.MessagePayloadJson!, // Value (сериализованный DomainEventEnvelope)
                            cancellationToken: stoppingToken);

                        // Если отправка успешна, помечаем запись как опубликованную
                        message.IsPublished = true;
                    }
                    catch (Exception ex)
                    {
                        // Если Kafka недоступна или произошла ошибка сети, мы НЕ помечаем сообщение как IsPublished.
                        // В следующем цикле (через 3 секунды) воркер снова попытается его отправить.
                        logger.LogError(ex,
                            "Ошибка публикации сообщения {IdempotencyKey} (Тип: {MessageType}). Будет повторная попытка.",
                            message.IdempotencyKey, message.MessageType);
                    }
                }

                // Сохраняем изменения (IsPublished = true) для всего пакета за один запрос к БД
                await dbContext.SaveChangesAsync(stoppingToken);

                logger.LogInformation("Пакет событий успешно обработан и сохранен в БД.");
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Корректное завершение работы приложения
                break;
            }
            catch (Exception ex)
            {
                // Глобальная ошибка (например, отвалилась БД). Ждем и пробуем снова.
                logger.LogError(ex, "Критическая ошибка в цикле KafkaOutboxPublisherWorker. Повтор через 5 секунд...");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}