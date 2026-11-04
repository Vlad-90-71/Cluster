namespace Cluster.Storage;

public sealed class SeaweedFsOptions
{
    public const string SectionName = "SeaweedFS";
    public string[] MasterUrls { get; set; } = [];
    public string[] FilerUrls { get; set; } = [];
    public string PublicUrl { get; set; } = string.Empty;
    public string FilerPublicUrl { get; set; } = string.Empty;
    public string Replication { get; set; } = "001";
}