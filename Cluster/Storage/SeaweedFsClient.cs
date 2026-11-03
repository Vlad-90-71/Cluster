using System.Net.Http.Json;
using Microsoft.Extensions.Options;

namespace Cluster.Storage;

public sealed class SeaweedFsClient(
    IOptions<SeaweedFsOptions> options,
    IHttpClientFactory httpClientFactory)
{
    private readonly SeaweedFsOptions _options = options.Value;
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;

    public async Task<SeaweedFsAssignment> AssignAsync(CancellationToken cancellationToken = default)
    {
        using var httpClient = _httpClientFactory.CreateClient();

        var errors = new List<string>();

        foreach (var masterUrl in _options.MasterUrls)
        {
            try
            {
                var url =
                    $"{masterUrl.TrimEnd('/')}/dir/assign" +
                    $"?replication={Uri.EscapeDataString(_options.Replication)}";

                using var response = await httpClient.GetAsync(url, cancellationToken);

                response.EnsureSuccessStatusCode();

                var assignment = await response.Content
                    .ReadFromJsonAsync<SeaweedFsAssignment>(
                        cancellationToken: cancellationToken);

                if (assignment is null ||
                    string.IsNullOrWhiteSpace(assignment.Fid))
                {
                    throw new InvalidOperationException(
                        "SeaweedFS Master вернул пустой FID.");
                }

                return assignment;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{masterUrl}: {ex.Message}");
            }
        }

        throw new InvalidOperationException(
            "Не удалось получить FID ни от одного SeaweedFS Master. " +
            string.Join(" | ", errors));
    }

    public async Task<string> UploadAsync(IFormFile file, CancellationToken cancellationToken = default)
    {
        if (file == null || file.Length == 0)
            throw new ArgumentException("Файл не выбран.", nameof(file));

        var assignment = await AssignAsync(cancellationToken);

        using var content = new MultipartFormDataContent();
        using Stream stream = file.OpenReadStream();

        using StreamContent fileContent = new(stream);

        fileContent.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue(
                string.IsNullOrWhiteSpace(file.ContentType)
                    ? "application/octet-stream"
                    : file.ContentType);

        content.Add(fileContent, "file", Path.GetFileName(file.FileName));

        using HttpClient httpClient = _httpClientFactory.CreateClient();

        var uploadUrl = $"{_options.PublicUrl.TrimEnd('/')}/{assignment.Fid}";

        using HttpResponseMessage response = await httpClient.PostAsync(uploadUrl, content, cancellationToken);

        response.EnsureSuccessStatusCode();

        return assignment.Fid;
    }
}