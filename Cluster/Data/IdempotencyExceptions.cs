namespace Cluster.Data;

public sealed class OperationNeedsInspectionException(string message) : Exception(message);
public sealed class IdempotencyConflictException : Exception
{
    public IdempotencyConflictException(string message) : base(message) { }

    public IdempotencyConflictException(string message, Exception innerException) : base(message, innerException) { }
}
