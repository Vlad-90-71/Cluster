namespace Cluster.Storage;

public sealed record TusUploadRequest(Guid UploadId, string FileName, long FileLength);
public sealed record TusUploadChunkRequest(string Location, long Offset, long ChunkLength);