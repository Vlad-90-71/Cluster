using System.ComponentModel.DataAnnotations;

namespace Cluster.Models;

public sealed class CreateCustomerViewModel
{
    public Guid IdempotencyKey { get; set; }

    [Required]
    [StringLength(200)]
    public string Name { get; set; } = string.Empty;

    [Required]
    [EmailAddress]
    [StringLength(320)]
    public string Email { get; set; } = string.Empty;
}