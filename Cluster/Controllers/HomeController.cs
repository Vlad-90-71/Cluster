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


        [HttpPost]
        [RequestSizeLimit(100 * 1024 * 1024)]
        public async Task<IActionResult> UploadFile(
            IFormFile file,
            CancellationToken cancellationToken)
        {
            if (file == null || file.Length == 0)
                return BadRequest("Файл не выбран.");

            try
            {
                var fid = await _seaweedFsClient.UploadAsync(file, cancellationToken);

                var downloadUrl =
                    $"{_seaweedFsPublicUrl.TrimEnd('/')}/{fid}";

                return Ok(new
                {
                    Message = "Файл успешно загружен в SeaweedFS!",
                    FileId = fid,
                    file.FileName,
                    file.ContentType,
                    FileSize = file.Length,
                    DownloadUrl = downloadUrl
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка загрузки файла в SeaweedFS.");

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
