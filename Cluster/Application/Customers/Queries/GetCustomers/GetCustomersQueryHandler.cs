using Cluster.Controllers;
using Cluster.Data;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using System.Text.Json;

namespace Cluster.Application.Customers.Queries.GetCustomers;

public sealed class GetCustomersQueryHandler(
    ReadDbContext context, 
    IDistributedCache cache, 
    ILogger<GetCustomersQueryHandler> logger)
    : IRequestHandler<GetCustomersQuery, List<Customer>>
{
    public const string CacheKey = "customers:all";
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);

    public async Task<List<Customer>> Handle(GetCustomersQuery request, CancellationToken cancellationToken)
    {
        // 1. Проверяем Redis.
        var cachedData = await cache.GetStringAsync(CacheKey, cancellationToken);

        if (cachedData is not null)
        {
            logger.LogInformation("Redis cache HIT: {CacheKey}", CacheKey);

            var cachedCustomers = JsonSerializer.Deserialize<List<Customer>>(cachedData);

            if (cachedCustomers is not null)
                return cachedCustomers;
        }

        logger.LogInformation("Redis cache MISS: {CacheKey}", CacheKey);

        // 2. Кэш отсутствует — читаем PostgreSQL.
        var customers = await context.Customers
            .AsNoTracking()
            .OrderBy(x => x.Id)
            .ToListAsync(cancellationToken);

        // 3. Сохраняем результат в Redis на 5 минут.
        var serializedCustomers = JsonSerializer.Serialize(customers);

        await cache.SetStringAsync(
            CacheKey,
            serializedCustomers,
            new DistributedCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = CacheDuration
            },
            cancellationToken);

        return customers;
    }
}