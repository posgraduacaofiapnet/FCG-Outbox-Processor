-- Eventos que esgotaram as dez tentativas e não serão mais selecionados pelo worker.
SELECT Id, EventType, CreatedAt, Attempts, NextAttemptAt, Payload
FROM dbo.OutboxMessages
WHERE IsSuccessful = 0
  AND Attempts >= 10
ORDER BY CreatedAt, Id;

-- Reprocessamento manual de um evento após a causa da falha ser corrigida.
-- Sempre informe explicitamente o Id do evento que foi analisado.
DECLARE @Id uniqueidentifier = NULL; -- Substitua NULL pelo Id aprovado para reprocessamento.

UPDATE dbo.OutboxMessages
SET Attempts = 0,
    NextAttemptAt = NULL
WHERE Id = @Id
  AND IsSuccessful = 0
  AND Attempts >= 10;
