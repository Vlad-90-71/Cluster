using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

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
public sealed record TransactionExecutionResult(
    bool IsSuccess,
    TransactionFailureReason FailureReason = TransactionFailureReason.None,
    string? ResultJson = null,
    string? ErrorMessage = null,
    Guid? ConflictingExecutionId = null // Для отладки: кто уже использует этот ключ
);
public static class DbTransactionExecutor
{
    private const string IdempotencyIndexName = "IX_TransactionOperations_IdempotencyKey";

    public static async Task<TransactionExecutionResult> ExecuteAsync(
        DbContext context,
        Guid idempotencyKey,
        Func<DbContext, CancellationToken, Task<string>> operation,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(logger);

        if (idempotencyKey == Guid.Empty)
            return new TransactionExecutionResult(false,
                TransactionFailureReason.UnknownError,
                ErrorMessage: "Idempotency key cannot be empty.");

        // Фиксируем ExecutionId для текущего бизнес-вызова. 
        // При повторных попытках внутри strategy.ExecuteAsync он обязан оставаться неизменным.
        var executionId = Guid.NewGuid();
        var strategy = context.Database.CreateExecutionStrategy();

        try
        {
            return await strategy.ExecuteAsync(async ct =>
            {
                // Очищаем локальный кэш контекста перед каждым витком повторной попытки, 
                // чтобы избежать склеивания старых состояний сущностей.
                context.ChangeTracker.Clear();

                await using var transaction = await context.Database.BeginTransactionAsync(ct);

                // 1. Пытаемся взять неблокирующую рекомендательную транзакционную блокировку.
                // Возвращает true, если залочено успешно, или false, если ключ занят другим процессом ПРЯМО СЕЙЧАС.
                var isLocked = await context.Database
                    .SqlQuery<bool>($"SELECT pg_try_advisory_xact_lock(hashtextextended({idempotencyKey.ToString("D")}, 0))")
                    .SingleAsync(ct);

                if (!isLocked)
                {
                    logger.LogWarning(
                        "Parallel request detected. Advisory lock is busy. Key: {Key}",
                        idempotencyKey);

                    return new TransactionExecutionResult(false,
                        TransactionFailureReason.IdempotencyConflict,
                        ErrorMessage: "Операция с таким ключом уже выполняется в другом потоке. Попробуйте позже.");
                }

                // 2. Блокировка успешна, проверяем статус в таблице истории
                var existing = await context.Set<TransactionOperation>()
                    .AsNoTracking()
                    .SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, ct);

                if (existing is not null)
                {
                    // Если результат уже записан, просто отдаем его (идеальный сценарий повтора от клиента)
                    if (!string.IsNullOrWhiteSpace(existing.ResultJson))
                    {
                        logger.LogInformation(
                            "Idempotent result returned from cache. Key: {Key}", 
                            idempotencyKey);
                        
                        return new TransactionExecutionResult(true, ResultJson: existing.ResultJson);
                    }

                    // Запись есть, но JSON пустой. Проверяем, КТО ее оставил.
                    // Если это наш же executionId — значит, это повторный виток ExecutionStrategy после сетевого сбоя. Мы можем продолжить.
                    if (existing.ExecutionId != executionId)
                    {
                        logger.LogWarning(
                            "Idempotency conflict detected. Key: {Key}, Existing ExecutionId: {ExistingId}, Current ExecutionId: {CurrentId}",
                            idempotencyKey, existing.ExecutionId, executionId);

                        return new TransactionExecutionResult(false,
                            TransactionFailureReason.IdempotencyConflict,
                            ErrorMessage: "Операция с таким ключом была начата другим процессом и прервана.",
                            ConflictingExecutionId: existing.ExecutionId);
                    }

                    logger.LogInformation(
                        "Execution strategy retry detected for the same execution. Key: {Key}, ExecutionId: {ExecutionId}", 
                        idempotencyKey, executionId);
                }
                else
                {
                    // Если записи нет вообще, создаем «черновик», фиксируя факт начала выполнения операции данным потоком.
                    // Это спасет, если в процессе работы operation() база данных "отвалится" и блокировка снимется.
                    var draft = new TransactionOperation
                    {
                        IdempotencyKey = idempotencyKey,
                        ExecutionId = executionId,
                        ResultJson = string.Empty, // пока пусто
                        CreatedAtUtc = DateTime.UtcNow
                    };
                    context.Set<TransactionOperation>().Add(draft);
                    await context.SaveChangesAsync(ct);
                }

                // 3. Выполнение бизнес-логики
                var json = await operation(context, ct);

                if (string.IsNullOrWhiteSpace(json))
                {
                    return new TransactionExecutionResult(false,
                        TransactionFailureReason.UnknownError,
                        ErrorMessage: "Операция вернула пустой результат.");
                }

                // 4. Обновляем существующий черновик или финально записываем результат
                var recordToUpdate = context.ChangeTracker.Entries<TransactionOperation>()
                    .Select(e => e.Entity)
                    .FirstOrDefault(x => x.IdempotencyKey == idempotencyKey)
                    ?? await context.Set<TransactionOperation>()
                        .SingleAsync(x => x.IdempotencyKey == idempotencyKey, ct);

                recordToUpdate.ResultJson = json;
                recordToUpdate.CreatedAtUtc = DateTime.UtcNow; // обновляем время завершения

                context.Set<TransactionOperation>().Update(recordToUpdate);
                await context.SaveChangesAsync(ct);

                // Фиксация транзакции — снимет advisory lock автоматически
                await transaction.CommitAsync(ct);

                return new TransactionExecutionResult(true, ResultJson: json);
            }, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Transaction execution was canceled by timeout or token. Key: {Key}", idempotencyKey);
            throw;
        }
        catch (DbUpdateException ex) when (IsIdempotencyUniqueViolation(ex))
        {
            logger.LogWarning(
                "Database unique constraint violation on idempotency key. Key: {Key}", 
                idempotencyKey);

            return new TransactionExecutionResult(false, 
                TransactionFailureReason.DatabaseConstraintError, 
                ErrorMessage: "Нарушение уникальности ключа идемпотентности.");
        }
        catch (NpgsqlException npgsqlEx) when (npgsqlEx.SqlState == "25006") // Read-only transaction
        {
            logger.LogWarning(
                "Database is temporarily read-only (possible Patroni failover). Key: {Key}", 
                idempotencyKey);

            return new TransactionExecutionResult(false, 
                TransactionFailureReason.DatabaseTransientError, 
                ErrorMessage: "База данных временно недоступна для записи. Повторите попытку.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, 
                "Unexpected error during transaction execution. Key: {Key}, ExecutionId: {ExecutionId}",
                idempotencyKey, executionId);

            return new TransactionExecutionResult(false, 
                TransactionFailureReason.UnknownError, 
                ErrorMessage: "Произошла непредвиденная ошибка при выполнении операции.");
        }
    }

    private static bool IsIdempotencyUniqueViolation(DbUpdateException exception) =>
        exception.InnerException is PostgresException pg
        && pg.SqlState == PostgresErrorCodes.UniqueViolation
        && pg.ConstraintName == IdempotencyIndexName;

    private static async Task AcquireTransactionLockAsync(DbContext context, Guid idempotencyKey, CancellationToken cancellationToken)
    {
        var connection = context.Database.GetDbConnection();
        var transaction = context.Database.CurrentTransaction
            ?? throw new InvalidOperationException("A transaction must be active before acquiring the advisory lock.");

        await using var command = connection.CreateCommand();
        command.Transaction = transaction.GetDbTransaction();
        command.CommandText = "SELECT pg_advisory_xact_lock(hashtextextended(@key, 0));";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "key";
        parameter.Value = idempotencyKey.ToString("D");
        command.Parameters.Add(parameter);

        await command.ExecuteScalarAsync(cancellationToken);
    }
}