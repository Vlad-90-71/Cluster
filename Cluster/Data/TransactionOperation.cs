
namespace Cluster.Data;

public sealed class TransactionOperation
{
    public long Id { get; set; }

    // Один ключ на одну логическую операцию.
    public Guid IdempotencyKey { get; set; }
    public Guid ExecutionId { get; set; }
    public string? MessageType { get; set; }
    public string? MessagePayloadJson { get; set; }
    public bool IsPublished { get; set; } = false;
    public string? ResultJson { get; set; } = null;
    public DateTime CreatedAtUtc { get; set; }
}
