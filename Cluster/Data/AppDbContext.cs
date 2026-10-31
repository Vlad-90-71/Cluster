
using Microsoft.EntityFrameworkCore;

namespace Cluster.Data;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<TransactionOperation> TransactionOperations  => Set<TransactionOperation>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<TransactionOperation>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.HasIndex(x => x.IdempotencyKey)
                .IsUnique();

            entity.Property(x => x.ExecutionId)
                .IsRequired();

            entity.Property(x => x.ResultJson)
                .HasColumnType("jsonb");
        });
    }
}
