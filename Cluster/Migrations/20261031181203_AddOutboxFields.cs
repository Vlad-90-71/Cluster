using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cluster.Migrations
{
    /// <inheritdoc />
    public partial class AddOutboxFields : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsPublished",
                table: "TransactionOperations",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<string>(
                name: "MessagePayloadJson",
                table: "TransactionOperations",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "MessageType",
                table: "TransactionOperations",
                type: "text",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IsPublished",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "MessagePayloadJson",
                table: "TransactionOperations");

            migrationBuilder.DropColumn(
                name: "MessageType",
                table: "TransactionOperations");
        }
    }
}
