using Cluster.Data;
using Microsoft.Extensions.Logging;

namespace Cluster.Messaging.Kafka;

public sealed class DomainEventDispatcher(
    DomainEventHandlerRegistry eventRegistry,
    ILogger<DomainEventDispatcher> logger)
{
    /// <summary>
    /// Диспетчеризирует событие к нужному обработчику.
    /// </summary>
    /// <returns>
    /// true, если событие успешно обработано или для него нет обработчика (следует сделать Commit).
    /// false, если произошла ошибка обработки (следует отправить в DLQ).
    /// </returns>
    public async Task<bool> DispatchAsync(
        AppDbContext dbContext,
        DomainEventEnvelope envelope,
        object payload,
        CancellationToken cancellationToken)
    {
        if (!eventRegistry.TryGetHandler(envelope.EventType, out var handler) || handler is null)
        {
            logger.LogWarning("Не найден обработчик для EventType: {EventType}. Сообщение будет проигнорировано.", envelope.EventType);
            return true; // Нет обработчика = не ошибка, просто коммитим, чтобы не зациклить
        }

        // 🚀 Делегируем всю сложную логику транзакций, блокировок и идемпотентности
        var result = await DbTransactionExecutor.CompleteAsync(
            dbContext,
            envelope.MessageId,
            (_, ct) => handler.HandleAsync(payload, ct),
            logger,
            cancellationToken);

        if (result.IsSuccess)
        {
            logger.LogInformation("Событие {MessageId} успешно обработано.", envelope.MessageId);
            return true;
        }

        logger.LogWarning("Обработка события {MessageId} не удалась: {Reason}. {ErrorMessage}",
            envelope.MessageId, result.FailureReason, result.ErrorMessage);

        return false; // Ошибка = возвращаем false, чтобы воркер отправил сообщение в DLQ
    }
}