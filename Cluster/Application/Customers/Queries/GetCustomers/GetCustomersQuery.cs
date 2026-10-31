using Cluster.Data;
using MediatR;

namespace Cluster.Application.Customers.Queries.GetCustomers;

public sealed record GetCustomersQuery : IRequest<List<Customer>>;