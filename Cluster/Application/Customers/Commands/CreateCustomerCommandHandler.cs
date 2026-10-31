using Cluster.Application.Customers.Queries.GetCustomers;
using Cluster.Data;
using Cluster.Messaging.Kafka;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using System.Text.Json;

namespace Cluster.Application.Customers.Commands;

public sealed record CreateCustomerCommand(Guid IdempotencyKey, string Name, string Email)
    : IRequest<CustomerCreationResult>;

public sealed record CustomerCreationResult(
    bool IsSuccess,
    long? CustomerId = null,
    string? Name = null,
    string? Email = null,
    string? ErrorMessage = null,
    TransactionFailureReason FailureReason = TransactionFailureReason.None);

public sealed class CreateCustomerCommandHandler(
    AppDbContext dbContext,
    IDistributedCache cache,
    ILogger<CreateCustomerCommandHandler> logger)
    : IRequestHandler<CreateCustomerCommand, CustomerCreationResult>
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task<CustomerCreationResult> Handle(CreateCustomerCommand request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        var transactionResult = await DbTransactionExecutor.ExecuteAsync(
            context: dbContext,
            idempotencyKey: request.IdempotencyKey,
            operation: async (context, ct) =>
            {
                var db = (AppDbContext)context;
                var customer = new Customer { Name = request.Name.Trim(), Email = request.Email.Trim() };
                db.Customers.Add(customer);
                await db.SaveChangesAsync(ct);

                // Находим черновик транзакции, созданный DbTransactionExecutor
                var txRecord = db.ChangeTracker.Entries<TransactionOperation>()
                    .Select(e => e.Entity)
                    .FirstOrDefault(x => x.IdempotencyKey == request.IdempotencyKey)
                    ?? await db.Set<TransactionOperation>()
                        .SingleOrDefaultAsync(x => x.IdempotencyKey == request.IdempotencyKey, ct);

                if (txRecord != null)
                {
                    // 1. Создаем контракт для Kafka
                    var envelope = new DomainEventEnvelope
                    {
                        EventType = "CustomerCreated",
                        MessageId = request.IdempotencyKey,
                        PayloadJson = JsonSerializer.Serialize(new { customer.Id, customer.Name, customer.Email }),
                        OccurredAtUtc = DateTime.UtcNow
                    };

                    // 2. Сохраняем метку для удобства SQL-запросов (опционально, но полезно)
                    txRecord.MessageType = "CustomerCreated";

                    // 3. Сохраняем ГОТОВОЕ сообщение для Kafka в базу
                    txRecord.MessagePayloadJson = JsonSerializer.Serialize(envelope);
                }

                return JsonSerializer.Serialize(new { customer.Id, customer.Name, customer.Email }, JsonOptions);
            },
            logger: logger,
            cancellationToken: cancellationToken);

        if (!transactionResult.IsSuccess)
        {
            // Трансляция инфраструктурного результата в доменный
            return new CustomerCreationResult(
                IsSuccess: false,
                ErrorMessage: transactionResult.ErrorMessage,
                FailureReason: transactionResult.FailureReason);
        }

        // Успех: инвалидируем кэш
        logger.LogInformation("Транзакция успешна. Инвалидация кэша: {CacheKey}", GetCustomersQueryHandler.CacheKey);
        await cache.RemoveAsync(GetCustomersQueryHandler.CacheKey, cancellationToken);

        // Парсим результат для возврата
        var resultData = JsonSerializer.Deserialize<JsonElement>(transactionResult.ResultJson!);
        return new CustomerCreationResult(
            IsSuccess: true,
            CustomerId: resultData.GetProperty("id").GetInt64(),
            Name: resultData.GetProperty("name").GetString(),
            Email: resultData.GetProperty("email").GetString());
    }
}