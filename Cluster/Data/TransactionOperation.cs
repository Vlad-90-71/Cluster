
namespace Cluster.Data;

public sealed class TransactionOperation
{
    public long Id { get; set; }

    // Один ключ на одну логическую операцию.
    public Guid IdempotencyKey { get; set; }
    public Guid ExecutionId { get; set; }

    public string? ResultTypeName { get; set; }
    public string? ResultJson { get; set; } = null;

    public string? MessageType { get; set; }
    public bool IsPublished { get; set; } = false;

    // Механизм захвата Publisher.
    public Guid? ClaimId { get; set; }
    public DateTime? LockedUntilUtc { get; set; }
    public int AttemptCount { get; set; }

    public bool IsProcessed { get; set; } = false;
    public DateTime? ProcessedAtUtc { get; set; }

    public DateTime CreatedAtUtc { get; set; }
}
