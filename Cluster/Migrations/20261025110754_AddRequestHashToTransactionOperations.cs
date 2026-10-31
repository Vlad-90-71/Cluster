using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cluster.Migrations
{
    /// <inheritdoc />
    public partial class AddRequestHashToTransactionOperations : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "RequestHash",
                table: "TransactionOperations",
                type: "character varying(64)",
                maxLength: 64,
                nullable: false,
                defaultValue: "");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "RequestHash",
                table: "TransactionOperations");
        }
    }
}
