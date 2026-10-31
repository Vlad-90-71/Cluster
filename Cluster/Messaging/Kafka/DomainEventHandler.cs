using System.Text.Json;

namespace Cluster.Messaging.Kafka;

// 1. Единый интерфейс для всех обработчиков событий
public interface IDomainEventHandler
{
    string SupportedEventType { get; }
    Task HandleAsync(string payloadJson, CancellationToken ct);
}

// 2. Реестр, который хранит словарь "Тип события -> Обработчик"
public sealed class DomainEventHandlerRegistry
{
    private readonly Dictionary<string, IDomainEventHandler> _handlers = new(StringComparer.OrdinalIgnoreCase);

    // Конструктор автоматически собирает все обработчики из DI-контейнера
    public DomainEventHandlerRegistry(IEnumerable<IDomainEventHandler> handlers)
    {
        foreach (var handler in handlers)
        {
            _handlers[handler.SupportedEventType] = handler;
        }
    }

    public bool TryGetHandler(string eventType, out IDomainEventHandler? handler)
    {
        return _handlers.TryGetValue(eventType, out handler);
    }
}