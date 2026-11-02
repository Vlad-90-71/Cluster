using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cluster.Migrations
{
    /// <inheritdoc />
    public partial class AddTransactionOperationClaim : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "AttemptCount",
                table: "TransactionOperations",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<Guid>(
                name: "ClaimId",
                table: "TransactionOperations",
                type: "uuid",
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "LockedUntilUtc",
                table: "TransactionOperations",
                type: "timestamp with time zone",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "AttemptCount",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "ClaimId",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "LockedUntilUtc",
                table: "TransactionOperations");
        }
    }
}
