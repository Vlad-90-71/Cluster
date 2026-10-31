using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cluster.Migrations
{
    /// <inheritdoc />
    public partial class AddExecutionIdToTransactionOperations : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "CustomerId",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "LastError",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "OperationType",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "RequestHash",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "Status",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "UpdatedAtUtc",
                table: "TransactionOperations");

            migrationBuilder.AddColumn<Guid>(
                name: "ExecutionId",
                table: "TransactionOperations",
                type: "uuid",
                nullable: false,
                defaultValue: new Guid("00000000-0000-0000-0000-000000000000"));

            migrationBuilder.AddColumn<string>(
                name: "ResultJson",
                table: "TransactionOperations",
                type: "jsonb",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ExecutionId",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "ResultJson",
                table: "TransactionOperations");

            migrationBuilder.AddColumn<int>(
                name: "CustomerId",
                table: "TransactionOperations",
                type: "integer",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "LastError",
                table: "TransactionOperations",
                type: "character varying(2000)",
                maxLength: 2000,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OperationType",
                table: "TransactionOperations",
                type: "character varying(100)",
                maxLength: 100,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "RequestHash",
                table: "TransactionOperations",
                type: "character varying(64)",
                maxLength: 64,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "Status",
                table: "TransactionOperations",
                type: "character varying(30)",
                maxLength: 30,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<DateTime>(
                name: "UpdatedAtUtc",
                table: "TransactionOperations",
                type: "timestamp with time zone",
                nullable: false,
                defaultValue: new DateTime(1, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified));
        }
    }
}
