using System.Text.Json;

namespace Cluster.Messaging.Kafka;

// 1. Единый интерфейс для всех обработчиков событий
public interface IDomainEventHandler
{
    string SupportedEventType { get; }
    Task HandleAsync(object payload, CancellationToken ct);
}

// 2. Реестр, который хранит словарь "Тип события -> Обработчик"
public sealed class DomainEventHandlerRegistry
{
    private readonly ILogger<DomainEventHandlerRegistry> _logger;

    private readonly Dictionary<string, IDomainEventHandler> _handlers = new(StringComparer.OrdinalIgnoreCase);
   
    public DomainEventHandlerRegistry(
        IEnumerable<IDomainEventHandler> handlers, 
        ILogger<DomainEventHandlerRegistry> logger)
    {
        _logger = logger;

        foreach (var handler in handlers)
        {
            _handlers[handler.SupportedEventType] = handler;
        }

        // 🚀 ЭТА СТРОКА СКАЖЕТ НАМ ВСЮ ПРАВДУ ПРИ ЗАПУСКЕ:
        _logger.LogInformation("🔍 ЗАГРУЖЕНО ОБРАБОТЧИКОВ: {Count}. Найденные типы: {Types}",
            _handlers.Count, string.Join(", ", _handlers.Keys));
    }

    public bool TryGetHandler(string eventType, out IDomainEventHandler? handler) =>
        _handlers.TryGetValue(eventType, out handler);
}