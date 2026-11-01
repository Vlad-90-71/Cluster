using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using MediatR;
using Cluster.Data;
using Cluster.Application.Customers.Queries.GetCustomers;

namespace Cluster.Application.Customers.Commands;

// 1. Строгий тип для результата
public sealed record CustomerCreatedData(long Id, string Name, string Email);

public sealed record CreateCustomerCommand(Guid IdempotencyKey, string Name, string Email)
    : IRequest<CustomerCreationResult>;

public sealed record CustomerCreationResult(
    bool IsSuccess,
    CustomerCreatedData? Customer = null,
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

        // 🚀 УКАЗЫВАЕМ ТИП РЕЗУЛЬТАТА: <CustomerCreatedData>
        var transactionResult = await DbTransactionExecutor.ExecuteAsync(
            context: dbContext,
            idempotencyKey: request.IdempotencyKey,
            operation: async (context, ct) =>
            {
                var db = (AppDbContext)context;
                var customer = new Customer { Name = request.Name.Trim(), Email = request.Email.Trim() };
                db.Customers.Add(customer);
                await db.SaveChangesAsync(ct); // Сохраняем Customer, чтобы получить Id

                // 🚀 ВОЗВРАЩАЕМ КОРТЕЖ: (Бизнес-результат, Тип события для Kafka)
                return (
                    Result: new CustomerCreatedData(customer.Id, customer.Name, customer.Email),
                    MessageType: "CustomerCreated"
                );
            },
            logger: logger,
            jsonOptions: JsonOptions,
            cancellationToken: cancellationToken);

        if (!transactionResult.IsSuccess)
        {
            return new CustomerCreationResult(
                IsSuccess: false,
                ErrorMessage: transactionResult.ErrorMessage,
                FailureReason: transactionResult.FailureReason);
        }

        logger.LogInformation("Транзакция успешна. Инвалидация кэша: {CacheKey}", GetCustomersQueryHandler.CacheKey);
        await cache.RemoveAsync(GetCustomersQueryHandler.CacheKey, cancellationToken);

        return new CustomerCreationResult(IsSuccess: true, Customer: transactionResult.Result!);
    }
}