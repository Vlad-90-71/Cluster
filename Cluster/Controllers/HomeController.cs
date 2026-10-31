using Cluster.Models;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace Cluster.Controllers
{
    public class HomeController(IWebHostEnvironment env, ILogger<HomeController> logger) : Controller
    {
        private readonly IWebHostEnvironment _env = env;
        private readonly ILogger<HomeController> _logger = logger;

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }
        
        [HttpPost]
        public async Task<IActionResult> UploadFile(IFormFile file)
        {
            if (file == null || file.Length == 0)
                return Content("Файл не выбран");

            var uploadsFolder = Path.Combine(_env.WebRootPath, "uploads");

            Directory.CreateDirectory(uploadsFolder);

            // Генерируем уникальное имя, сохраняя расширение файла.
            var extension = Path.GetExtension(Path.GetFileName(file.FileName));
            var fileName = $"{Guid.NewGuid():N}{extension}";
            var filePath = Path.Combine(uploadsFolder, fileName);

            await using (var stream = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                await file.CopyToAsync(stream);

            return Content($"Файл успешно загружен! Ссылка: /uploads/{fileName}");
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
