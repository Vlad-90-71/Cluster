
namespace Cluster.Data;

public sealed class TransactionOperation
{
    public long Id { get; set; }

    // Один ключ на одну логическую операцию.
    public Guid IdempotencyKey { get; set; }
    public Guid ExecutionId { get; set; }
    public string? ResultJson { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
