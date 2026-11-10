using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Medallion.Threading;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;

namespace Cluster.Storage;

public sealed record SeaweedFsUploadResult(
    Guid UploadId,
    string Path,
    string DownloadUrl);

public sealed record TusUploadSession(
    Guid UploadId,
    string Path,
    string Location,
    long Length,
    string DownloadUrl);

public sealed record TusUploadProgress(
    long Offset,
    long Length,
    string FilerUrl);

public sealed class SeaweedFsClient(
    IOptions<SeaweedFsOptions> options,
    IHttpClientFactory httpClientFactory,
    IDistributedCache cache,
    IDistributedLockProvider lockProvider)
{
    private readonly SeaweedFsOptions _options = options.Value;
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;
    private readonly IDistributedCache _cache = cache;
    private readonly IDistributedLockProvider _lockProvider = lockProvider;

    private static readonly TimeSpan SessionCacheTtl = TimeSpan.FromDays(7);
    private const long MaxTusChunkLength = 4L * 1024 * 1024;

    private sealed record TusRoute(
        string FilerUrl,
        long Length);

    private static string GetCacheKey(string location)
    {
        var hash = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(location)));

        return $"Tus:Filer:{hash}";
    }

    private static string NormalizeFilerUrl(string filerUrl)
    {
        return filerUrl.TrimEnd('/');
    }

    private bool IsConfiguredFiler(string filerUrl)
    {
        return _options.FilerUrls.Any(
            configured => string.Equals(
                NormalizeFilerUrl(configured),
                NormalizeFilerUrl(filerUrl),
                StringComparison.OrdinalIgnoreCase));
    }

    private async Task<TusRoute?> GetCachedRouteAsync(
        string location,
        CancellationToken cancellationToken)
    {
        try
        {
            var json = await _cache.GetStringAsync(
                GetCacheKey(location),
                cancellationToken);

            if (string.IsNullOrWhiteSpace(json))
                return null;

            var route = JsonSerializer.Deserialize<TusRoute>(json);

            if (route is null ||
                string.IsNullOrWhiteSpace(route.FilerUrl) ||
                route.Length <= 0 ||
                !IsConfiguredFiler(route.FilerUrl))
            {
                return null;
            }

            return route with
            {
                FilerUrl = NormalizeFilerUrl(route.FilerUrl)
            };
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // При недоступности Redis используем обнаружение через HEAD.
            return null;
        }
    }

    private async Task SaveRouteAsync(
        string location,
        string filerUrl,
        long length,
        CancellationToken cancellationToken)
    {
        if (!IsConfiguredFiler(filerUrl) || length <= 0)
            return;

        try
        {
            var route = new TusRoute(
                NormalizeFilerUrl(filerUrl),
                length);

            var json = JsonSerializer.Serialize(route);

            await _cache.SetStringAsync(
                GetCacheKey(location),
                json,
                new DistributedCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = SessionCacheTtl
                },
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Redis — это кеш маршрутизации, а не источник истины.
            // При его недоступности маршрут можно восстановить через HEAD.
        }
    }

    private async Task<HttpClient> CreateHttpClientAsync()
    {
        return _httpClientFactory.CreateClient();
    }

    public async Task<SeaweedFsUploadResult> UploadAsync(
        IFormFile file,
        Guid uploadId,
        CancellationToken cancellationToken = default)
    {
        if (file is null || file.Length == 0)
            throw new ArgumentException("Файл не выбран.", nameof(file));

        var fileName = Path.GetFileName(file.FileName);

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Некорректное имя файла.", nameof(file));

        if (uploadId == Guid.Empty)
            throw new ArgumentException("Некорректный идентификатор загрузки.", nameof(uploadId));

        var filerPath =
            $"/uploads/{uploadId:D}/{Uri.EscapeDataString(fileName)}";

        using var httpClient = _httpClientFactory.CreateClient();

        var errors = new List<string>();

        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = NormalizeFilerUrl(filerUrl);
            var fileUrl = $"{baseUrl}{filerPath}";
            var directoryUrl = $"{baseUrl}/uploads/{uploadId:D}/";

            try
            {
                using var checkRequest =
                    new HttpRequestMessage(HttpMethod.Get, fileUrl);

                checkRequest.Headers.TryAddWithoutValidation(
                    "Accept",
                    "application/octet-stream");

                using var checkResponse = await httpClient.SendAsync(
                    checkRequest,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                if (checkResponse.StatusCode == HttpStatusCode.OK)
                {
                    return new SeaweedFsUploadResult(
                        uploadId,
                        filerPath,
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

                using var response = await httpClient.PostAsync(
                    directoryUrl,
                    content,
                    cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    return new SeaweedFsUploadResult(
                        uploadId,
                        filerPath,
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

    public async Task<TusUploadSession> CreateTusUploadAsync(
        Guid uploadId,
        string fileName,
        long fileLength,
        CancellationToken cancellationToken = default)
    {
        if (uploadId == Guid.Empty)
            throw new ArgumentException(
                "Некорректный идентификатор загрузки.",
                nameof(uploadId));

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException(
                "Некорректное имя файла.",
                nameof(fileName));

        if (fileLength <= 0)
            throw new ArgumentException(
                "Размер файла должен быть больше нуля.",
                nameof(fileLength));

        fileName = Path.GetFileName(fileName);

        var filerPath =
            $"/uploads/{uploadId:D}/{Uri.EscapeDataString(fileName)}";

        using var httpClient = _httpClientFactory.CreateClient();

        var errors = new List<string>();

        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = NormalizeFilerUrl(filerUrl);
            var createUrl = $"{baseUrl}/.tus{filerPath}";

            try
            {
                using var request =
                    new HttpRequestMessage(HttpMethod.Post, createUrl);

                request.Headers.TryAddWithoutValidation(
                    "Tus-Resumable",
                    "1.0.0");

                request.Headers.TryAddWithoutValidation(
                    "Upload-Length",
                    fileLength.ToString(
                        System.Globalization.CultureInfo.InvariantCulture));

                var encodedFileName = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(fileName));

                request.Headers.TryAddWithoutValidation(
                    "Upload-Metadata",
                    $"filename {encodedFileName}");

                using var response = await httpClient.SendAsync(
                    request,
                    cancellationToken);

                if (response.StatusCode != HttpStatusCode.Created)
                {
                    errors.Add(
                        $"{filerUrl}: создание TUS-сессии вернуло " +
                        $"HTTP {(int)response.StatusCode}.");

                    continue;
                }

                var location = response.Headers.Location?.ToString();

                if (string.IsNullOrWhiteSpace(location))
                {
                    errors.Add($"{filerUrl}: Filer не вернул Location.");
                    continue;
                }

                if (Uri.TryCreate(
                    location,
                    UriKind.Absolute,
                    out var absoluteUri))
                {
                    location = absoluteUri.PathAndQuery;
                }

                if (!location.StartsWith(
                    "/.tus/",
                    StringComparison.Ordinal))
                {
                    errors.Add($"{filerUrl}: получен некорректный Location.");
                    continue;
                }

                // Сохраняем маршрут общей TUS-сессии в Redis.
                await SaveRouteAsync(
                    location,
                    baseUrl,
                    fileLength,
                    cancellationToken);

                return new TusUploadSession(
                    uploadId,
                    filerPath,
                    location,
                    fileLength,
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
            "Не удалось создать TUS-сессию. " +
            string.Join(" | ", errors));
    }

    private async Task<TusUploadProgress?> TryGetProgressFromFilerAsync(
        string location,
        string filerUrl,
        CancellationToken cancellationToken)
    {
        var baseUrl = NormalizeFilerUrl(filerUrl);
        var sessionUrl = $"{baseUrl}{location}";

        using var httpClient = _httpClientFactory.CreateClient();

        using var request =
            new HttpRequestMessage(HttpMethod.Head, sessionUrl);

        request.Headers.TryAddWithoutValidation(
            "Tus-Resumable",
            "1.0.0");

        using var response = await httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);

        if (response.StatusCode != HttpStatusCode.OK)
            return null;

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
            return null;
        }

        if (parsedOffset < 0 ||
            parsedLength <= 0 ||
            parsedOffset > parsedLength)
        {
            return null;
        }

        return new TusUploadProgress(
            parsedOffset,
            parsedLength,
            baseUrl);
    }

    public async Task<TusUploadProgress> GetTusProgressAsync(
        string location,
        CancellationToken cancellationToken = default)
    {
        ValidateLocation(location);

        var errors = new List<string>();
        var checkedFilers = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);

        // Сначала проверяем Filer, сохранённый в Redis.
        var cachedRoute = await GetCachedRouteAsync(
            location,
            cancellationToken);

        if (cachedRoute is not null)
        {
            var preferredUrl = NormalizeFilerUrl(cachedRoute.FilerUrl);
            checkedFilers.Add(preferredUrl);

            try
            {
                var progress = await TryGetProgressFromFilerAsync(
                    location,
                    preferredUrl,
                    cancellationToken);

                if (progress is not null)
                {
                    await SaveRouteAsync(
                        location,
                        progress.FilerUrl,
                        progress.Length,
                        cancellationToken);

                    return progress;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{preferredUrl}: {ex.Message}");
            }
        }

        // Если предпочтительный Filer недоступен,
        // ищем сессию на остальных Filers.
        foreach (var filerUrl in _options.FilerUrls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = NormalizeFilerUrl(filerUrl);

            if (!checkedFilers.Add(baseUrl))
                continue;

            try
            {
                var progress = await TryGetProgressFromFilerAsync(
                    location,
                    baseUrl,
                    cancellationToken);

                if (progress is null)
                {
                    errors.Add($"{baseUrl}: TUS-сессия не найдена.");
                    continue;
                }

                await SaveRouteAsync(
                    location,
                    progress.FilerUrl,
                    progress.Length,
                    cancellationToken);

                return progress;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{baseUrl}: {ex.Message}");
            }
        }

        throw new InvalidOperationException(
            "Не удалось получить прогресс TUS-загрузки. " +
            string.Join(" | ", errors));
    }


    public async Task<TusUploadProgress> UploadTusChunkAsync(
        string location,
        long offset,
        Stream chunkStream,
        long chunkLength,
        CancellationToken cancellationToken = default)
    {
        ValidateLocation(location);

        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkLength);
        ArgumentNullException.ThrowIfNull(chunkStream);

        if (chunkLength > MaxTusChunkLength)
            throw new ArgumentOutOfRangeException(
                nameof(chunkLength),
                $"Размер чанка не должен превышать {MaxTusChunkLength} байт.");

        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, long.MaxValue - chunkLength);

        var expectedOffset = offset + chunkLength;

        // Читаем входящий HTTP-поток только один раз.
        // Все повторные PATCH будут использовать этот буфер.
        var chunkBytes = await ReadChunkAsync(
            chunkStream,
            chunkLength,
            cancellationToken);

        var lockName = $"SeaweedFS:TUS:{GetCacheKey(location)}";

        await using var sessionLock = await _lockProvider
            .CreateLock(lockName)
            .AcquireAsync(cancellationToken: cancellationToken);

        var errors = new List<string>();

        // Получаем актуальное состояние сессии.
        // При отказе vm-db GetTusProgressAsync переключится на vm-app.
        var current = await GetTusProgressAsync(
            location,
            cancellationToken);

        if (current.Offset >= expectedOffset)
            return current;

        if (current.Offset != offset)
        {
            throw new InvalidOperationException(
                $"Смещение TUS не совпадает. " +
                $"Ожидалось {offset}, получено {current.Offset}.");
        }

        if (expectedOffset > current.Length)
        {
            throw new InvalidOperationException(
                "Размер части превышает размер файла.");
        }

        // Сначала пробуем Filer, на котором обнаружена сессия.
        // Затем Redis-маршрут и остальные настроенные Filer.
        var filers = new List<string>
    {
        current.FilerUrl
    };

        var cachedRoute = await GetCachedRouteAsync(
            location,
            cancellationToken);

        if (cachedRoute is not null)
            filers.Add(cachedRoute.FilerUrl);

        filers.AddRange(_options.FilerUrls);

        var checkedFilers = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);

        using var httpClient = _httpClientFactory.CreateClient();

        foreach (var filerUrl in filers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseUrl = NormalizeFilerUrl(filerUrl);

            if (!IsConfiguredFiler(baseUrl) ||
                !checkedFilers.Add(baseUrl))
            {
                continue;
            }

            TusUploadProgress? filerProgress;

            // Перед PATCH убеждаемся, что конкретный Filer
            // видит эту сессию с нужным offset.
            try
            {
                filerProgress = await TryGetProgressFromFilerAsync(
                    location,
                    baseUrl,
                    cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{baseUrl}: HEAD: {ex.Message}");
                continue;
            }

            if (filerProgress is null)
            {
                errors.Add($"{baseUrl}: TUS-сессия не найдена.");
                continue;
            }

            if (filerProgress.Length != current.Length)
            {
                errors.Add($"{baseUrl}: размер TUS-сессии отличается.");
                continue;
            }

            if (filerProgress.Offset >= expectedOffset)
            {
                await SaveRouteAsync(
                    location,
                    baseUrl,
                    filerProgress.Length,
                    cancellationToken);

                return filerProgress;
            }

            if (filerProgress.Offset != offset)
            {
                errors.Add(
                    $"{baseUrl}: неожиданный Upload-Offset " +
                    $"{filerProgress.Offset}.");

                continue;
            }

            try
            {
                using var request = new HttpRequestMessage(
                    HttpMethod.Patch,
                    $"{baseUrl}{location}");

                request.Headers.TryAddWithoutValidation(
                    "Tus-Resumable",
                    "1.0.0");

                request.Headers.TryAddWithoutValidation(
                    "Upload-Offset",
                    offset.ToString(
                        System.Globalization.CultureInfo.InvariantCulture));

                // Для каждого PATCH создаётся новое содержимое.
                using var content = new ByteArrayContent(chunkBytes);

                content.Headers.ContentType =
                    new System.Net.Http.Headers.MediaTypeHeaderValue(
                        "application/offset+octet-stream");

                request.Content = content;

                using var response = await httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                if (response.StatusCode == HttpStatusCode.NoContent &&
                    TryGetTusHeader(
                        response,
                        "Upload-Offset",
                        out var newOffset) &&
                    long.TryParse(
                        newOffset,
                        System.Globalization.NumberStyles.None,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out var parsedOffset) &&
                    parsedOffset == expectedOffset)
                {
                    await SaveRouteAsync(
                        location,
                        baseUrl,
                        current.Length,
                        cancellationToken);

                    return new TusUploadProgress(
                        parsedOffset,
                        current.Length,
                        baseUrl);
                }

                errors.Add(
                    $"{baseUrl}: PATCH вернул HTTP " +
                    $"{(int)response.StatusCode} или некорректный Upload-Offset.");
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add($"{baseUrl}: PATCH: {ex.Message}");
            }

            // PATCH мог быть принят сервером, даже если ответ потерялся.
            // Поэтому прежде чем пробовать другой Filer, проверяем offset.
            var resolved = await ResolvePatchResultAsync(
                location,
                offset,
                expectedOffset,
                cancellationToken);

            if (resolved is not null)
                return resolved;

            // HEAD подтвердил, что offset не изменился.
            // Можно безопасно попробовать следующий Filer.
        }

        throw new InvalidOperationException(
            "Не удалось передать чанк через доступные Filer. " +
            string.Join(" | ", errors));
    }

    private static async Task<byte[]> ReadChunkAsync(Stream chunkStream, long chunkLength, CancellationToken cancellationToken)
    {
        if (chunkLength > MaxTusChunkLength)
            throw new ArgumentOutOfRangeException(
                nameof(chunkLength),
                $"Размер чанка не должен превышать {MaxTusChunkLength} байт.");

        var buffer = new byte[checked((int)chunkLength)];
        var totalRead = 0;

        while (totalRead < buffer.Length)
        {
            var read = await chunkStream.ReadAsync(
                buffer.AsMemory(totalRead, buffer.Length - totalRead),
                cancellationToken);

            if (read == 0)
                throw new EndOfStreamException(
                    $"Тело чанка короче ожидаемого размера. " +
                    $"Получено {totalRead} из {chunkLength} байт.");

            totalRead += read;
        }

        return buffer;
    }

    private async Task<TusUploadProgress?> ResolvePatchResultAsync(
        string location,
        long originalOffset,
        long expectedOffset,
        CancellationToken cancellationToken)
    {
        // После неоднозначного PATCH обязательно проверяем состояние
        // через HEAD. Если состояние определить нельзя — не повторяем PATCH.
        var actual = await GetTusProgressAsync(
            location,
            cancellationToken);

        if (actual.Offset >= expectedOffset)
            return actual;

        if (actual.Offset == originalOffset)
            return null;

        throw new InvalidOperationException(
            $"После PATCH получено неожиданное смещение TUS: " +
            $"{actual.Offset}. Ожидалось {originalOffset} или не менее {expectedOffset}.");
    }

    private static void ValidateLocation(string location)
    {
        if (string.IsNullOrWhiteSpace(location) ||
            !location.StartsWith("/.tus/", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Некорректный Location TUS-сессии.",
                nameof(location));
        }
    }

    private static bool TryGetTusHeader(
        HttpResponseMessage response,
        string name,
        out string value)
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