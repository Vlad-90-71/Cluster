using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cluster.Migrations
{
    /// <inheritdoc />
    public partial class AddTransactionOperationProcessing : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsProcessed",
                table: "TransactionOperations",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<DateTime>(
                name: "ProcessedAtUtc",
                table: "TransactionOperations",
                type: "timestamp with time zone",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IsProcessed",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "ProcessedAtUtc",
                table: "TransactionOperations");
        }
    }
}
