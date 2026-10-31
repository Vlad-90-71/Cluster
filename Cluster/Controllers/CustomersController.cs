using Cluster.Application.Customers.Commands.CreateCustomer;
using Cluster.Application.Customers.Queries.GetCustomers;
using Cluster.Data;
using Cluster.Messaging.Kafka;
using Cluster.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Cluster.Controllers;

public sealed class CustomersController(
    ISender sender,
    KafkaProducer producer,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<CustomersController> logger) 
    : Controller
{
    private readonly ISender _sender = sender;
    private readonly KafkaProducer _producer = producer;
    private readonly KafkaOptions _kafkaOptions = kafkaOptions.Value;
    private readonly ILogger<CustomersController> _logger = logger;

    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var replica = Environment.GetEnvironmentVariable("HOSTNAME") ?? Environment.MachineName;
        _logger.LogInformation("Customers/Index handled by replica {Replica}", replica);

        var customers = await _sender.Send(new GetCustomersQuery(), cancellationToken);
        return View(customers);
    }

    [HttpGet]
    public IActionResult Create()
    {
        return View(new CreateCustomerViewModel { IdempotencyKey = Guid.NewGuid() });
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(CreateCustomerViewModel model, CancellationToken cancellationToken)
    {
        if (model.IdempotencyKey == Guid.Empty)
            ModelState.AddModelError(nameof(model.IdempotencyKey), "Отсутствует идентификатор операции.");

        if (!ModelState.IsValid)
            return View(model);

        var message = new CreateCustomerMessage(
            MessageId: model.IdempotencyKey,
            Name: model.Name.Trim(),
            Email: model.Email.Trim(),
            CreatedAtUtc: DateTime.UtcNow);

        try
        {
            var json = JsonSerializer.Serialize(message);

            await _producer.PublishAsync(
                _kafkaOptions.Topics.CreateCustomer,
                message.MessageId.ToString(),
                json,
                cancellationToken);

            _logger.LogInformation(
                "CreateCustomer message sent to Kafka. MessageId={MessageId}",
                message.MessageId);

            return RedirectToAction(nameof(Index));
        }
        catch (Confluent.Kafka.KafkaException ex)
        {
            _logger.LogError(ex,
                "Failed to publish CreateCustomer message. MessageId={MessageId}",
                message.MessageId);

            ModelState.AddModelError(string.Empty,
                "Не удалось отправить запрос на создание Customer. Попробуйте ещё раз.");

            return View(model);
        }
    }

    /*
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(CreateCustomerViewModel model, CancellationToken cancellationToken)
    {
        if (model.IdempotencyKey == Guid.Empty)
            ModelState.AddModelError(nameof(model.IdempotencyKey), "Отсутствует идентификатор операции.");

        if (!ModelState.IsValid)
            return View(model);

        // Отправляем команду и получаем структурированный результат
        var result = await _sender.Send(
            new CreateCustomerCommand(model.IdempotencyKey, model.Name, model.Email),
            cancellationToken);

        if (!result.IsSuccess)
        {
            // 1. Логируем техническую деталь для админа/разработчика
            _logger.LogWarning("Create customer failed. Reason: {Reason}, TechMessage: {TechMessage}, Key: {Key}",
                result.FailureReason, result.ErrorMessage, model.IdempotencyKey);

            // 2. Получаем безопасное сообщение для пользователя из единого центра
            string userMessage = TransactionFailureMessageResolver.GetUserMessage(
                result.FailureReason,
                result.ErrorMessage);

            // 3. Добавляем в ModelState и возвращаем форму
            ModelState.AddModelError(string.Empty, userMessage);
            return View(model);
        }

        // Успех
        return RedirectToAction(nameof(Index));
    }
    */
}