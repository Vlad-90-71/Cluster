namespace Cluster.Data;

/// <summary>
/// Отвечает за маппинг технических причин сбоев транзакций в понятные сообщения для пользовательского интерфейса.
/// Централизованное управление текстами ошибок для всех контроллеров.
/// </summary>
public static class TransactionFailureMessageResolver
{
    public static string GetUserMessage(TransactionFailureReason reason, string? technicalErrorMessage = null)
    {
        return reason switch
        {
            TransactionFailureReason.IdempotencyConflict =>
                "Этот ключ уже использован. Проверьте результат операции и начните новую только после проверки.",

            TransactionFailureReason.RequiresManualInspection =>
                "Результат операции требует проверки. Не создавайте новую операцию с другим ключом.",

            TransactionFailureReason.DatabaseTransientError =>
                "База данных временно недоступна для записи (возможно, идет переключение узлов). Повторите попытку через 10 секунд с тем же ключом.",

            TransactionFailureReason.DatabaseConstraintError =>
                // Если БД вернула конкретную причину (например, "Дубликат Email"), показываем её. Иначе - общую.
                string.IsNullOrWhiteSpace(technicalErrorMessage)
                    ? "Ошибка целостности данных. Проверьте корректность введенных значений."
                    : $"Ошибка данных: {technicalErrorMessage}",

            _ =>
                // Фоллбэк для любых непредвиденных ошибок (UnknownError и т.д.)
                $"Не удалось подтвердить результат операции. Не отправляйте новую операцию с другим ключом, пока результат предыдущей не проверен."
        };
    }
}