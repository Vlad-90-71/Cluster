using System.Net;
using Microsoft.Extensions.Options;

namespace Cluster.Storage;

public sealed record SeaweedFsUploadResult(Guid UploadId, string Path, string DownloadUrl);
public sealed record TusUploadSession(Guid UploadId, string Path, string Location, long Length, string DownloadUrl);
public sealed record TusUploadProgress(long Offset, long Length, string FilerUrl);


public sealed class SeaweedFsClient(IOptions<SeaweedFsOptions> options, IHttpClientFactory httpClientFactory)
{
    private readonly SeaweedFsOptions _options = options.Value;
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;

    public async Task<SeaweedFsUploadResult> UploadAsync(IFormFile file, Guid uploadId, CancellationToken cancellationToken = default)
    {
        if (file is null || file.Length == 0)
            throw new ArgumentException("Файл не выбран.", nameof(file));

        var fileName = Path.GetFileName(file.FileName);

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Некорректное имя файла.", nameof(file));

        if (uploadId == Guid.Empty)
            throw new ArgumentException("Некорректный идентификатор загрузки.", nameof(uploadId));
        
        var filerPath = $"/uploads/{uploadId:D}/{Uri.EscapeDataString(fileName)}";

        using var httpClient = _httpClientFactory.CreateClient();
        var errors = new List<string>();

        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = filerUrl.TrimEnd('/');
            var fileUrl = $"{baseUrl}{filerPath}";
            var directoryUrl = $"{baseUrl}/uploads/{uploadId:D}/";

            try
            {
                // Перед загрузкой проверяем, не сохранился ли файл
                // предыдущей попыткой, ответ на которую потерялся.
                using var checkRequest = new HttpRequestMessage(HttpMethod.Get, fileUrl);
                checkRequest.Headers.TryAddWithoutValidation("Accept", "application/octet-stream");

                using var checkResponse = await httpClient.SendAsync(
                    checkRequest,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                if (checkResponse.StatusCode == HttpStatusCode.OK)
                {
                    return new SeaweedFsUploadResult(uploadId, filerPath,
                        $"{_options.FilerPublicUrl.TrimEnd('/')}{filerPath}");
                }

                if (checkResponse.StatusCode != HttpStatusCode.NotFound)
                {
                    errors.Add(
                        $"{filerUrl}: проверка файла вернула " +
                        $"HTTP {(int)checkResponse.StatusCode}.");

                    continue;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{filerUrl}: проверка файла: {ex.Message}");
                continue;
            }

            try
            {
                using var content = new MultipartFormDataContent();
                await using var stream = file.OpenReadStream();
                using var fileContent = new StreamContent(stream);

                fileContent.Headers.ContentType =
                    new System.Net.Http.Headers.MediaTypeHeaderValue(
                        string.IsNullOrWhiteSpace(file.ContentType)
                            ? "application/octet-stream"
                            : file.ContentType);

                content.Add(fileContent, "file", fileName);

                using var response = await httpClient.PostAsync(directoryUrl, content, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    return new SeaweedFsUploadResult(uploadId,  filerPath,
                        $"{_options.FilerPublicUrl.TrimEnd('/')}{filerPath}");
                }

                errors.Add(
                    $"{filerUrl}: загрузка вернула " +
                    $"HTTP {(int)response.StatusCode}.");
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{filerUrl}: загрузка: {ex.Message}");
            }
        }

        throw new InvalidOperationException(
            "Не удалось подтвердить загрузку файла через SeaweedFS Filer. " +
            string.Join(" | ", errors));
    }

    public async Task<TusUploadSession> CreateTusUploadAsync(Guid uploadId, string fileName, long fileLength, CancellationToken cancellationToken = default)
    {
        if (uploadId == Guid.Empty)
            throw new ArgumentException("Некорректный идентификатор загрузки.", nameof(uploadId));

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Некорректное имя файла.", nameof(fileName));

        if (fileLength <= 0)
            throw new ArgumentException("Размер файла должен быть больше нуля.", nameof(fileLength));

        fileName = Path.GetFileName(fileName);

        var filerPath = $"/uploads/{uploadId:D}/{Uri.EscapeDataString(fileName)}";

        using var httpClient = _httpClientFactory.CreateClient();

        var errors = new List<string>();

        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = filerUrl.TrimEnd('/');

            var createUrl = $"{baseUrl}/.tus{filerPath}";

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, createUrl);

                request.Headers.TryAddWithoutValidation("Tus-Resumable", "1.0.0");

                request.Headers.TryAddWithoutValidation(
                    "Upload-Length",
                    fileLength.ToString(System.Globalization.CultureInfo.InvariantCulture));

                var encodedFileName = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(fileName));
                request.Headers.TryAddWithoutValidation("Upload-Metadata", $"filename {encodedFileName}");

                using var response = await httpClient.SendAsync(request, cancellationToken);

                if (response.StatusCode != HttpStatusCode.Created)
                {
                    errors.Add($"{filerUrl}: создание TUS-сессии вернуло HTTP {(int)response.StatusCode}.");

                    continue;
                }

                var location = response.Headers.Location?.ToString();

                if (string.IsNullOrWhiteSpace(location))
                {
                    errors.Add($"{filerUrl}: Filer не вернул Location.");

                    continue;
                }

                // Сохраняем путь сессии, а не адрес конкретной Filer.
                // Это позволит обращаться к той же сессии через другую ноду.
                if (Uri.TryCreate(location, UriKind.Absolute, out var absoluteUri))
                    location = absoluteUri.PathAndQuery;

                if (!location.StartsWith("/.tus/", StringComparison.Ordinal))
                {
                    errors.Add($"{filerUrl}: получен некорректный Location.");

                    continue;
                }

                return new TusUploadSession(uploadId, filerPath, location, fileLength,
                    $"{_options.FilerPublicUrl.TrimEnd('/')}{filerPath}");
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{filerUrl}: {ex.Message}");
            }
        }

        throw new InvalidOperationException(
            "Не удалось создать TUS-сессию. " + string.Join(" | ", errors));
    }

    public async Task<TusUploadProgress> GetTusProgressAsync(string location, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(location) ||
            !location.StartsWith("/.tus/", StringComparison.Ordinal))
        {
            throw new ArgumentException("Некорректный Location TUS-сессии.", nameof(location));
        }

        using var httpClient = _httpClientFactory.CreateClient();

        var errors = new List<string>();

        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = filerUrl.TrimEnd('/');
            var sessionUrl = $"{baseUrl}{location}";

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Head, sessionUrl);

                request.Headers.TryAddWithoutValidation("Tus-Resumable", "1.0.0");

                using var response = await httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    errors.Add($"{filerUrl}: HEAD вернул HTTP {(int)response.StatusCode}.");
                    continue;
                }

                if (!TryGetTusHeader(response, "Upload-Offset", out var offset) ||
                    !TryGetTusHeader(response, "Upload-Length", out var length) ||
                    !long.TryParse(
                        offset,
                        System.Globalization.NumberStyles.None,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out var parsedOffset) ||
                    !long.TryParse(
                        length,
                        System.Globalization.NumberStyles.None,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out var parsedLength))
                {
                    errors.Add($"{filerUrl}: отсутствуют корректные Upload-Offset или Upload-Length.");
                    continue;
                }

                if (parsedOffset < 0 || parsedLength < 0 || parsedOffset > parsedLength)
                {
                    errors.Add($"{filerUrl}: некорректные значения смещения или размера.");
                    continue;
                }

                return new TusUploadProgress(parsedOffset, parsedLength, filerUrl);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{filerUrl}: {ex.Message}");
            }
        }

        throw new InvalidOperationException(
            "Не удалось получить прогресс TUS-загрузки. " +
            string.Join(" | ", errors));
    }

    public async Task<long> UploadTusChunkAsync(
        string location,
        long offset,
        Stream chunkStream,
        long chunkLength,
        string filerUrl,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(location) ||
            !location.StartsWith("/.tus/", StringComparison.Ordinal))
        {
            throw new ArgumentException("Некорректный Location TUS-сессии.", nameof(location));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkLength);
        ArgumentNullException.ThrowIfNull(chunkStream);

        var baseUrl = filerUrl.TrimEnd('/');
        var sessionUrl = $"{baseUrl}{location}";

        using var httpClient = _httpClientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Patch, sessionUrl);

        request.Headers.TryAddWithoutValidation("Tus-Resumable", "1.0.0");
        request.Headers.TryAddWithoutValidation(
            "Upload-Offset",
            offset.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var content = new StreamContent(chunkStream);

        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/offset+octet-stream");

        content.Headers.ContentLength = chunkLength;

        request.Content = content;

        using var response = await httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode != HttpStatusCode.NoContent)
        {
            throw new InvalidOperationException(
                $"{filerUrl}: PATCH вернул HTTP {(int)response.StatusCode}.");
        }

        if (!TryGetTusHeader(response, "Upload-Offset", out var newOffset) ||
            !long.TryParse(
                newOffset,
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var parsedOffset))
        {
            throw new InvalidOperationException(
                "Filer не вернул корректный Upload-Offset после PATCH.");
        }

        return parsedOffset;
    }

    private static bool TryGetTusHeader(HttpResponseMessage response, string name, out string value)
    {
        if (response.Headers.TryGetValues(name, out var values))
        {
            value = values.FirstOrDefault() ?? string.Empty;
            return !string.IsNullOrWhiteSpace(value);
        }

        value = string.Empty;
        return false;
    }
}