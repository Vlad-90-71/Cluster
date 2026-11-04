using Cluster.Messaging.Kafka;
using Cluster.Models;
using Cluster.Storage;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace Cluster.Controllers
{
    public class HomeController(
        IWebHostEnvironment env, 
        ILogger<HomeController> logger,
        KafkaProducer kafkaProducer,
        DlqTestPublisher dlqTestPublisher,
        SeaweedFsClient seaweedFsClient,
        IOptions<SeaweedFsOptions> seaweedFsOptions) : Controller
    {
        private readonly IWebHostEnvironment _env = env;
        private readonly ILogger<HomeController> _logger = logger;
        
        private readonly KafkaProducer _kafkaProducer = kafkaProducer;
        private readonly DlqTestPublisher _dlqTestPublisher = dlqTestPublisher;
        private readonly SeaweedFsClient _seaweedFsClient = seaweedFsClient;
        private readonly string _seaweedFsPublicUrl = seaweedFsOptions.Value.PublicUrl;

        public IActionResult Index()
        {
            ViewData["UploadId"] = Guid.NewGuid();
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> TestDlq(CancellationToken cancellationToken)
        {
            await _dlqTestPublisher.PublishTestMessageAsync(cancellationToken);

            return Ok("Тестовое сообщение отправлено в DomainEvents.DLQ.");
        }

        [HttpPost]
        public async Task<IActionResult> TestDomainEvent(CancellationToken cancellationToken)
        {
            var message = System.Text.Json.JsonSerializer.Serialize(new
            {
                messageId = Guid.NewGuid(),
                eventType = "DLQ_TEST",
                payloadTypeName = "TestPayload",
                payloadJson = "{}"
            });

            await _kafkaProducer.PublishAsync(
                "DomainEvents",
                Guid.NewGuid().ToString(),
                message,
                cancellationToken);

            return Ok("Тестовое сообщение отправлено в DomainEvents.");
        }

        // Создание TUS-сессии.
        [HttpPost]
        public async Task<IActionResult> CreateUpload([FromBody] TusUploadRequest request, CancellationToken cancellationToken)
        {
            if (request.UploadId == Guid.Empty)
                return BadRequest("Некорректный идентификатор загрузки.");

            if (string.IsNullOrWhiteSpace(request.FileName))
                return BadRequest("Не указано имя файла.");

            if (request.FileLength <= 0 || request.FileLength > 100 * 1024 * 1024)
                return BadRequest("Недопустимый размер файла.");

            try
            {
                var session = await _seaweedFsClient.CreateTusUploadAsync(
                    request.UploadId,
                    request.FileName,
                    request.FileLength,
                    cancellationToken);

                return Ok(new
                {
                    session.UploadId,
                    session.Path,
                    session.Location,
                    session.Length,
                    session.DownloadUrl
                });
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Ошибка создания TUS-сессии. UploadId: {UploadId}",
                    request.UploadId);

                return Problem(
                    title: "Не удалось создать сессию загрузки.",
                    statusCode: StatusCodes.Status502BadGateway);
            }
        }


        // Получение прогресса TUS-загрузки.
        [HttpGet]
        public async Task<IActionResult> UploadProgress([FromQuery] string location, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(location))
                return BadRequest("Не указан Location TUS-сессии.");

            try
            {
                var progress = await _seaweedFsClient.GetTusProgressAsync(location, cancellationToken);

                return Ok(new
                {
                    progress.Offset,
                    progress.Length,
                    IsCompleted = progress.Offset == progress.Length
                });
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка получения прогресса TUS-загрузки.");

                return Problem(
                    title: "Не удалось получить прогресс загрузки.",
                    statusCode: StatusCodes.Status502BadGateway);
            }
        }


        // Передача очередной части файла.
        [HttpPatch]
        [RequestSizeLimit(4 * 1024 * 1024)]
        public async Task<IActionResult> UploadChunk(
            [FromQuery] string location,
            [FromQuery] long offset,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(location))
                return BadRequest("Не указан Location TUS-сессии.");

            if (offset < 0)
                return BadRequest("Некорректное смещение.");

            if (Request.ContentLength is not long chunkLength || chunkLength <= 0 || chunkLength > 4 * 1024 * 1024)
                return BadRequest("Недопустимый размер части файла.");

            try
            {
                // Сначала узнаём актуальное состояние сессии.
                // Это также позволяет выбрать доступную Filer.
                var progress = await _seaweedFsClient.GetTusProgressAsync(location, cancellationToken);

                if (offset != progress.Offset)
                {
                    return Conflict(new
                    {
                        Message = "Смещение загрузки изменилось.",
                        CurrentOffset = progress.Offset,
                        progress.Length
                    });
                }

                if (offset + chunkLength > progress.Length)
                    return BadRequest("Размер части превышает оставшийся размер файла.");

                var newOffset = await _seaweedFsClient.UploadTusChunkAsync(
                    location,
                    offset,
                    Request.Body,
                    chunkLength,
                    progress.FilerUrl,
                    cancellationToken);

                return Ok(new
                {
                    Offset = newOffset,
                    progress.Length,
                    IsCompleted = newOffset == progress.Length
                });
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Ошибка передачи части TUS-загрузки. Offset: {Offset}",
                    offset);

                // PATCH мог пройти на сервере, но ответ мог потеряться.
                // Поэтому проверяем фактическое смещение перед ответом клиенту.
                try
                {
                    var progress = await _seaweedFsClient.GetTusProgressAsync(location, cancellationToken);

                    if (progress.Offset == offset + chunkLength)
                    {
                        return Ok(new
                        {
                            progress.Offset,
                            progress.Length,
                            IsCompleted = progress.Offset == progress.Length
                        });
                    }

                    return StatusCode(
                        StatusCodes.Status502BadGateway,
                        new
                        {
                            Message = "Не удалось передать часть файла.",
                            CurrentOffset = progress.Offset,
                            progress.Length
                        });
                }
                catch (Exception progressException)
                {
                    _logger.LogError(
                        progressException,
                        "Не удалось проверить состояние TUS-сессии после ошибки PATCH.");

                    return Problem(
                        title: "Не удалось передать часть файла.",
                        statusCode: StatusCodes.Status502BadGateway);
                }
            }
        }

        [HttpPost]
        [RequestSizeLimit(100 * 1024 * 1024)]
        public async Task<IActionResult> UploadFile(IFormFile file, Guid uploadId, CancellationToken cancellationToken)
        {
            if (file == null || file.Length == 0)
                return BadRequest("Файл не выбран.");

            if (uploadId == Guid.Empty)
                return BadRequest("Некорректный идентификатор загрузки.");

            try
            {
                var result = await _seaweedFsClient.UploadAsync(file, uploadId, cancellationToken);

                return Ok(new
                {
                    Message = "Файл успешно загружен в SeaweedFS!",
                    result.UploadId,
                    result.Path,
                    file.FileName,
                    file.ContentType,
                    FileSize = file.Length,
                    result.DownloadUrl
                });
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Ошибка загрузки файла в SeaweedFS. UploadId: {UploadId}",
                    uploadId);

                return Problem(
                    title: "Не удалось загрузить файл в SeaweedFS.",
                    statusCode: StatusCodes.Status502BadGateway);
            }
        }
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
