namespace Cluster.Storage;

public sealed class SeaweedFsAssignment
{
    public string Fid { get; set; } = string.Empty;
    public string Url { get; set; } = string.Empty;
    public string PublicUrl { get; set; } = string.Empty;
    public int Count { get; set; }
}