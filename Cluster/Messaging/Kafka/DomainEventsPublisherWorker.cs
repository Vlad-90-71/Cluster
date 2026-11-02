using System.Text.Json;
using Microsoft.Extensions.Options;
using Microsoft.EntityFrameworkCore;
using Cluster.Data;

namespace Cluster.Messaging.Kafka;

public sealed class DomainEventsPublisherWorker(
    IServiceProvider serviceProvider,
    KafkaProducer kafkaProducer,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<DomainEventsPublisherWorker> logger) : BackgroundService
{
    private const int BatchSize = 50;
    private const int ClaimDuration = 60;

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
                var unpublishedMessages = await ClaimBatchAsync(dbContext, stoppingToken);

                if (unpublishedMessages.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                    continue;
                }

                var claimId = unpublishedMessages[0].ClaimId!.Value;

                logger.LogInformation(
                    "Захвачено {Count} событий для публикации. ClaimId: {ClaimId}",
                    unpublishedMessages.Length, claimId);

                int count = 0;
                var successfullyPublishedIds = new long[unpublishedMessages.Length];

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
                            PayloadTypeName = message.ResultTypeName!,
                            OccurredAtUtc = message.CreatedAtUtc
                        };

                        await kafkaProducer.PublishAsync(
                            topic: kafkaOptions.Value.Topic,
                            key: message.IdempotencyKey.ToString(),
                            message: JsonSerializer.Serialize(envelope),
                            cancellationToken: stoppingToken);

                        successfullyPublishedIds[count++] = message.Id;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Ошибка публикации сообщения {IdempotencyKey} (Тип: {MessageType}). Будет повторная попытка.",
                            message.IdempotencyKey, message.MessageType);
                    }
                }

                // 2. БЫСТРОЕ ОБНОВЛЕНИЕ БЕЗ ТРЕКИНГА
                if (count > 0)
                {
                    var idsForQuery = count == BatchSize 
                        ? successfullyPublishedIds 
                        : successfullyPublishedIds.AsSpan(0, count).ToArray();

                    await dbContext.TransactionOperations
                        .Where(x =>  idsForQuery.Contains(x.Id) && x.ClaimId == claimId && !x.IsPublished)
                        .ExecuteUpdateAsync(setters => setters
                            .SetProperty(x => x.IsPublished, true)
                            .SetProperty(x => x.ClaimId, (Guid?)null)
                            .SetProperty(x => x.LockedUntilUtc, (DateTime?)null),
                            stoppingToken);

                    logger.LogInformation("Успешно обновлен статус для {Count} событий.", count);
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

    private static async Task<TransactionOperation[]> ClaimBatchAsync(AppDbContext dbContext, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var claimId = Guid.NewGuid();
        var lockedUntil = now.AddSeconds(ClaimDuration);

        // Получаем кандидатов на захват.
        var candidateIds = await dbContext.TransactionOperations
            .AsNoTracking()
            .Where(x =>
                !x.IsPublished && x.MessageType != null && x.ResultJson != null && x.ResultTypeName != null &&
                (x.ClaimId == null || x.LockedUntilUtc <= now))
            .OrderBy(x => x.CreatedAtUtc)
            .Select(x => x.Id)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        int count = 0;
        var claimedIds = new long[BatchSize];


        foreach (var id in candidateIds)
        {
            // Условное атомарное обновление.
            var affectedRows = await dbContext.TransactionOperations
                .Where(x => x.Id == id && !x.IsPublished && (x.ClaimId == null || x.LockedUntilUtc <= now))
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(x => x.ClaimId, claimId)
                    .SetProperty(x => x.LockedUntilUtc, lockedUntil)
                    .SetProperty(x => x.AttemptCount, x => x.AttemptCount + 1),
                    cancellationToken);

            if (affectedRows == 1)
                claimedIds[count++] = id;
        }

        if (count == 0)
            return [];

        var idsForQuery = count == BatchSize ? claimedIds : claimedIds.AsSpan(0, count).ToArray();

        // Возвращаем только записи, захваченные текущим пакетом.
        return await dbContext.TransactionOperations
            .AsNoTracking()
            .Where(x => x.ClaimId == claimId && idsForQuery.Contains(x.Id))
            .OrderBy(x => x.CreatedAtUtc)
            .ToArrayAsync(cancellationToken);
    }
}