using Microsoft.EntityFrameworkCore;

namespace Cluster.Data;

public sealed class ReadDbContext(DbContextOptions<ReadDbContext> options) : DbContext(options)
{
    public DbSet<Customer> Customers => Set<Customer>();
}