using MediatR;

namespace Cluster.Application.Customers.Commands.CreateCustomer;

public sealed record CreateCustomerCommand(Guid IdempotencyKey, string Name, string Email)
    : IRequest<CustomerCreationResult>;