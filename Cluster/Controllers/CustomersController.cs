using Cluster.Application.Customers.Commands;
using Cluster.Application.Customers.Queries.GetCustomers;
using Cluster.Data;
using Cluster.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Cluster.Controllers;

public sealed class CustomersController(
    ISender sender,
    ILogger<CustomersController> logger)
    : Controller
{
    private readonly ISender _sender = sender;
    private readonly ILogger<CustomersController> _logger = logger;

    // GET: Customers
    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var replica = Environment.GetEnvironmentVariable("HOSTNAME") ?? Environment.MachineName;
        _logger.LogInformation("Customers/Index handled by replica {Replica}", replica);

        var customers = await _sender.Send(new GetCustomersQuery(), cancellationToken);
        return View(customers);
    }

    // GET: Customers/Create
    [HttpGet]
    public IActionResult Create()
    {
        // Генерируем новый IdempotencyKey для формы
        return View(new CreateCustomerViewModel { IdempotencyKey = Guid.NewGuid() });
    }

    // POST: Customers/Create
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(CreateCustomerViewModel model, CancellationToken cancellationToken)
    {
        // Валидация ключа идемпотентности
        if (model.IdempotencyKey == Guid.Empty)
            ModelState.AddModelError(nameof(model.IdempotencyKey), "Отсутствует идентификатор операции.");

        if (!ModelState.IsValid)
            return View(model);

        // Отправляем команду через MediatR
        var result = await _sender.Send(
            new CreateCustomerCommand(model.IdempotencyKey, model.Name, model.Email),
            cancellationToken);

        // Обработка результата
        if (!result.IsSuccess)
        {
            _logger.LogWarning(
                "Create customer failed. Reason: {Reason}, TechMessage: {TechMessage}, Key: {Key}",
                result.FailureReason, result.ErrorMessage, model.IdempotencyKey);

            // Получаем безопасное сообщение для пользователя
            string userMessage = TransactionFailureMessageResolver.GetUserMessage(
                result.FailureReason, result.ErrorMessage);

            ModelState.AddModelError(string.Empty, userMessage);
            return View(model);
        }

        // Успех: перенаправляем на список клиентов
        _logger.LogInformation(
            "Клиент успешно создан. ID: {CustomerId}, Name: {Name}",
            result.CustomerId, result.Name);

        TempData["SuccessMessage"] = $"Клиент {result.Name} успешно создан!";
        return RedirectToAction(nameof(Index));
    }
}