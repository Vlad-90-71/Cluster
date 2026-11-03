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

                        if (message.AttemptCount >= 5)
                        {
                            logger.LogCritical("Сообщение {IdempotencyKey} превысило лимит попыток ({Count}). Требуется ручное вмешательство или DLQ.",
                                message.IdempotencyKey, message.AttemptCount);
                            // Здесь можно было бы сделать отдельный ExecuteUpdate, чтобы пометить его как Failed, 
                            // но для простоты можно оставить как есть: оно будет пытаться каждые 60 сек.
                        }
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

        // ШАГ 1: Получаем кандидатов (только ID для минимизации трафика)
        var candidateIds = await dbContext.TransactionOperations
            .AsNoTracking()
            .Where(x =>
                !x.IsPublished && x.MessageType != null && x.ResultJson != null && x.ResultTypeName != null &&
                (x.ClaimId == null || x.LockedUntilUtc <= now))
            .OrderBy(x => x.CreatedAtUtc)
            .Select(x => x.Id)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        if (candidateIds.Count == 0)
            return [];

        // ШАГ 2: ОДИН атомарный UPDATE для всех кандидатов сразу (Решаем проблему N+1)
        var claimedCount = await dbContext.TransactionOperations
            .Where(x =>
                candidateIds.Contains(x.Id) &&
                !x.IsPublished &&
                (x.ClaimId == null || x.LockedUntilUtc <= now))
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(x => x.ClaimId, claimId)
                    .SetProperty(x => x.LockedUntilUtc, lockedUntil)
                    .SetProperty(x => x.AttemptCount, x => x.AttemptCount + 1),
                cancellationToken);

        if (claimedCount == 0)
            return [];

        // ШАГ 3: Возвращаем только те записи, которые реально удалось захватить этим ClaimId
        return await dbContext.TransactionOperations
            .AsNoTracking()
            .Where(x => x.ClaimId == claimId)
            .OrderBy(x => x.CreatedAtUtc)
            .ToArrayAsync(cancellationToken);
    }
}