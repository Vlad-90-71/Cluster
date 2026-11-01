using Medallion.Threading;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using System.Text.Json;

namespace Cluster.Data;

public enum TransactionFailureReason
{
    None,
    IdempotencyConflict,       // Ключ уже занят другой операцией
    RequiresManualInspection,  // Операция начата, но результат утерян/незавершен
    DatabaseTransientError,    // Временная ошибка БД (например, во время failover Patroni)
    DatabaseConstraintError,   // Ошибка ограничений БД (уникальность и т.д.)
    UnknownError               // Неожиданная ошибка
}

public sealed record TransactionExecutionResult<TResult>(
    bool IsSuccess,
    TransactionFailureReason FailureReason = TransactionFailureReason.None,
    TResult? Result = default,
    string? ErrorMessage = null,
    Guid? ConflictingExecutionId = null // Для отладки: кто уже использует этот ключ
);

//public sealed record OperationOutcome<TResult>(TResult Result, string? MessageType = null);

public static class DbTransactionExecutor
{
    private static IDistributedLockProvider? _lockProvider;

    public static void Initialize(IDistributedLockProvider lockProvider)
    {
        _lockProvider = lockProvider ?? throw new ArgumentNullException(nameof(lockProvider));
    }

    public static async Task<TransactionExecutionResult<TResult>> ExecuteAsync<TResult>(
        DbContext context,
        Guid idempotencyKey,
        Func<DbContext, CancellationToken, Task<(TResult Result, string? MessageType)>> operation, 
        ILogger logger,
        JsonSerializerOptions? jsonOptions = null,
        CancellationToken cancellationToken = default) where TResult : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(logger);

        if (_lockProvider == null)
            throw new InvalidOperationException("DbTransactionExecutor не был инициализирован.");

        if (idempotencyKey == Guid.Empty)
            return new TransactionExecutionResult<TResult>(false, ErrorMessage: "Idempotency key cannot be empty.");

        var executionId = Guid.NewGuid();
        var lockKey = $"lock:idempotency:{idempotencyKey:D}";
        var distributedLock = _lockProvider.CreateLock(lockKey);

        await using var lockHandle = await distributedLock.TryAcquireAsync(TimeSpan.Zero, cancellationToken);

        if (lockHandle == null)
        {
            logger.LogWarning("Parallel request detected. Redis lock is busy. Key: {Key}", idempotencyKey);
            return new TransactionExecutionResult<TResult>(false,
                FailureReason: TransactionFailureReason.IdempotencyConflict,
                ErrorMessage: "Операция с таким ключом уже выполняется.");
        }

        var strategy = context.Database.CreateExecutionStrategy();
        var options = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

        try
        {
            return await strategy.ExecuteAsync(async ct =>
            {
                context.ChangeTracker.Clear();
                await using var transaction = await context.Database.BeginTransactionAsync(ct);

                var existing = await context.Set<TransactionOperation>()
                    .AsNoTracking()
                    .SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, ct);

                if (existing is not null)
                {
                    if (!string.IsNullOrWhiteSpace(existing.ResultJson))
                    {
                        var cachedResult = JsonSerializer.Deserialize<TResult>(existing.ResultJson, options);
                        logger.LogInformation("Idempotent result returned from cache. Key: {Key}, Type: {Type}",
                            idempotencyKey, existing.ResultTypeName);

                        return new TransactionExecutionResult<TResult>(true, Result: cachedResult);
                    }

                    logger.LogWarning("Idempotency conflict detected (invalid state). Key: {Key}", idempotencyKey);
                    return new TransactionExecutionResult<TResult>(false,
                        FailureReason: TransactionFailureReason.IdempotencyConflict,
                        ErrorMessage: "Операция находится в некорректном состоянии.",
                        ConflictingExecutionId: existing.ExecutionId);
                }

                // 1. Выполняем бизнес-логику и распаковываем кортеж
                var (resultData, messageType) = await operation(context, ct);

                // 2. Создаем запись Outbox ОДНИМ МАХОМ со всеми данными
                var operationRecord = new TransactionOperation
                {
                    IdempotencyKey = idempotencyKey,
                    ExecutionId = executionId,
                    ResultJson = JsonSerializer.Serialize(resultData, options),
                    ResultTypeName = typeof(TResult).AssemblyQualifiedName,
                    CreatedAtUtc = DateTime.UtcNow,
                    MessageType = messageType, // 🚀 Берем из кортежа
                    IsPublished = false
                };

                context.Set<TransactionOperation>().Add(operationRecord);

                // 3. ЕДИНСТВЕННЫЙ SaveChanges: сохраняет и бизнес-сущности, и Outbox
                await context.SaveChangesAsync(ct);

                await transaction.CommitAsync(ct);

                return new TransactionExecutionResult<TResult>(true, Result: resultData);
            }, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Transaction execution was canceled. Key: {Key}", idempotencyKey);
            throw;
        }
        catch (DbUpdateException ex) when (IsIdempotencyUniqueViolation(ex))
        {
            logger.LogWarning("Database unique constraint violation on idempotency key. Key: {Key}", idempotencyKey);
            return new TransactionExecutionResult<TResult>(false,
                FailureReason: TransactionFailureReason.DatabaseConstraintError,
                ErrorMessage: "Ошибка уникальности ключа в базе данных.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error during idempotent transaction execution. Key: {Key}", idempotencyKey);
            return new TransactionExecutionResult<TResult>(false,
                FailureReason: TransactionFailureReason.UnknownError,
                ErrorMessage: ex.Message);
        }
    }

    private static bool IsIdempotencyUniqueViolation(DbUpdateException ex)
    {
        return ex.Entries != null && ex.Entries.Any(entry =>
            entry.Entity is TransactionOperation && entry.State == EntityState.Added);
    }
}