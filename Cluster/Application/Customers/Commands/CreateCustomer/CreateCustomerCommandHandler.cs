using Cluster.Application.Customers.Queries.GetCustomers;
using Cluster.Data;
using MediatR;
using Microsoft.Extensions.Caching.Distributed;
using System.Text.Json;

namespace Cluster.Application.Customers.Commands.CreateCustomer;

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
            CustomerId: resultData.GetProperty("Id").GetInt64(),
            Name: resultData.GetProperty("Name").GetString(),
            Email: resultData.GetProperty("Email").GetString());
    }
}