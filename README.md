# FCG Outbox Processor

Worker genérico da Fase 3 responsável por publicar na fila SQS os eventos persistidos em
`dbo.OutboxMessages`. O mesmo artefato roda uma vez para o banco de usuários e outra para
o banco de catálogo; nenhuma regra de evento fica neste processo.

## Fluxo

1. A API grava o recurso de negócio e o registro de outbox na mesma transação SQL.
2. O worker reivindica um lote elegível de forma atômica com `UPDLOCK`, `READPAST` e `ROWLOCK`.
3. A reivindicação incrementa `Attempts`, inclusive quando a primeira publicação tem sucesso.
4. O payload JSON é enviado integralmente no envelope `{ id, eventType, createdAt, payload }`.
5. Em sucesso, `IsSuccessful` recebe `true` e `NextAttemptAt` é limpo.
6. Em erro, `IsSuccessful` permanece `false` e `NextAttemptAt` recebe UTC + 15 minutos.
7. Após a décima tentativa sem sucesso, o evento sai da busca e exige intervenção manual.
8. Registros concluídos são removidos em lotes após o período de retenção configurado.

O intervalo também funciona como lease: se o processo cair depois da reivindicação, outro pod
poderá tentar novamente quando `NextAttemptAt` vencer. A entrega é, portanto, pelo menos uma vez;
a Lambda mantém a idempotência pelo `Id` do evento.

Eventos esgotados permanecem armazenados com `IsSuccessful = false` e `Attempts >= 10`.
As consultas de diagnóstico e reprocessamento controlado estão em `docs/outbox-operations.sql`.

## Configuração

| Chave | Descrição | Padrão |
|---|---|---:|
| `Outbox__ConnectionString` | Banco que contém `dbo.OutboxMessages` | obrigatório |
| `Outbox__QueueUrl` | URL da fila SQS consumida pela Lambda | obrigatório |
| `Outbox__Region` | Região AWS | `us-east-1` |
| `Outbox__BatchSize` | Quantidade máxima reivindicada | `20` |
| `Outbox__PollIntervalSeconds` | Intervalo de leitura | `5` |
| `Outbox__RetryDelayMinutes` | Lease e espera após falha | `15` |
| `Outbox__SuccessfulRetentionDays` | Retenção de concluídos | `7` |

Credenciais AWS não são armazenadas no repositório. Em Kubernetes, substitua `ACCOUNT_ID` na
anotação IRSA do ServiceAccount e associe à role a permissão `sqs:SendMessage` para a fila.

## Kubernetes

Atualize a URL no `k8s/configmap.yaml`, disponibilize a imagem
`fcg-outbox-processor:latest` e aplique:

```bash
kubectl apply -k k8s
```

Os deployments reutilizam os secrets já existentes das APIs para selecionar seu respectivo banco.
Os endpoints `/health/live`, `/health/ready` e `/metrics` ficam disponíveis apenas nos Services internos.

Os testes unitários estão em `tests/FCG.Outbox.Processor.Tests` e serão executados na etapa de
validação posterior, juntamente com a coleta de cobertura por arquivo.
