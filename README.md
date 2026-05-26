# Search Trends Service

Сервис для виджета "Сейчас ищут". Он читает поисковые события из Kafka, считает популярные запросы за последние 5 минут и отдает готовый Top-N через HTTP/JSON.

Главная идея решения: чтений топа ожидается сильно больше, чем входящих событий, поэтому ручка `/trends` не пересчитывает статистику на каждый запрос. Сервис заранее поддерживает in-memory snapshot топа и отдает его максимально дешево.

## Что Реализовано

- Kafka consumer для входящего потока поисковых событий
- in-memory sliding window за последние 5 минут
- быстрый `GET /trends?limit=N`
- динамический stop-list без перезапуска сервиса
- базовая защита от грубой накрутки
- Prometheus-метрики на `/metrics`
- `/healthz` и `/readyz`
- unit-тесты и benchmarks
- `docker-compose` для локального запуска Kafka и сервиса

## Быстрый Запуск

```bash
docker compose up --build
```

После запуска сервис доступен на `localhost:8080`.

Быстрая проверка:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl "http://localhost:8080/trends?limit=10"
curl http://localhost:8080/metrics
```

В compose поднимается Kafka. Topic `search-events` создается автоматически отдельным init-контейнером.

Kafka доступна:

- внутри compose-сети: `kafka:9092`
- с хоста: `localhost:29092`

## Как Закинуть Тестовые События

Можно отправить пачку событий через `kafka-console-producer` внутри Kafka-контейнера:

```bash
docker compose exec kafka bash -lc 'for i in $(seq 1 20); do ts=$(date -u +%Y-%m-%dT%H:%M:%SZ); echo "{\"event_id\":\"event-$i\",\"query\":\"iphone 15 pro\",\"user_id\":\"user-$i\",\"session_id\":\"session-$i\",\"timestamp\":\"$ts\"}"; done | kafka-console-producer.sh --bootstrap-server kafka:9092 --topic search-events'
```

Через секунду snapshot обновится, и топ можно проверить:

```bash
curl "http://localhost:8080/trends?limit=10"
```

## API

### Получить Тренды

```http
GET /trends?limit=10
```

`limit` задает количество элементов в ответе. По умолчанию используется `10`, максимум сейчас `100`.

Пример:

```bash
curl "http://localhost:8080/trends?limit=5"
```

Ответ:

```json
{
  "window_seconds": 300,
  "limit": 5,
  "generated_at": "2026-05-25T12:05:00Z",
  "items": [
    {
      "query": "iphone 15 pro",
      "count": 128
    }
  ]
}
```

`generated_at` показывает время сборки snapshot. Это важно: ответ может быть не на текущую миллисекунду, а на последний обновленный snapshot.

### Stop-list

Stop-list нужен, чтобы быстро скрывать нежелательные запросы из виджета. Он применяется к нормализованной строке запроса.

Добавить запрос:

```bash
curl -X POST http://localhost:8080/stop-list \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"iphone 15 pro\"}"
```

Посмотреть список:

```bash
curl http://localhost:8080/stop-list
```

Удалить запрос:

```bash
curl -X DELETE "http://localhost:8080/stop-list/iphone%2015%20pro"
```

После добавления в stop-list запрос скрывается из `/trends` сразу. Физически старые счетчики из агрегатора не удаляются: фильтрация происходит на выдаче. Это проще, быстрее и не требует пересобирать все окно.

### Health И Readiness

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

`/healthz` отвечает, что HTTP-процесс жив. `/readyz` оставлен как отдельная точка для readiness-проверок, чтобы позже туда можно было добавить более строгую проверку зависимостей.

### Метрики

```bash
curl http://localhost:8080/metrics
```

Основные метрики:

- `search_trends_kafka_messages_total`
- `search_trends_http_requests_total`
- `search_trends_http_request_duration_seconds`
- `search_trends_aggregation_queries`
- `search_trends_stop_list_size`
- `search_trends_snapshot_items`
- `search_trends_events_filtered_total`
- `search_trends_event_processing_errors_total`

## Kafka-Событие

Формат входящего сообщения выбран так, чтобы сервис мог считать окно по реальному времени события и делать базовую защиту от повторной накрутки.

```json
{
  "event_id": "01JZ8K7F4Q2V9K7ZK5Q3R1T8AA",
  "query": "iphone 15 pro",
  "user_id": "user-123",
  "session_id": "session-456",
  "timestamp": "2026-05-25T12:00:00Z"
}
```

Поля:

- `event_id`: уникальный id события
- `query`: исходный поисковый запрос
- `user_id`: id пользователя, если он известен
- `session_id`: fallback, если пользователя нельзя определить
- `timestamp`: время фактического поискового события

`timestamp` специально находится в payload, потому что время доставки в Kafka и время самого поиска могут отличаться. Для трендов важнее именно время пользовательского действия.

## Как Считается Популярность

Популярность равна количеству валидных событий с одинаковым нормализованным query за последние 5 минут.

Перед учетом query нормализуется:

- убираются пробелы по краям
- строка приводится к lower-case
- повторяющиеся пробелы схлопываются
- пустые строки отбрасываются
- длина ограничивается 256 символами

Я сознательно не добавлял лемматизацию, исправление опечаток и семантическое объединение запросов. Для тестового задания это был бы отдельный большой пласт логики, а здесь важнее показать надежную потоковую агрегацию и быстрые чтения.

## Архитектура

Общий pipeline выглядит так:

```text
Kafka
  -> Consumer
  -> Parse / Normalize
  -> Anti-abuse
  -> Sliding Window Aggregator
  -> Snapshot
  -> HTTP API
```

Агрегатор хранит данные в секундных bucket-ах:

```text
bucket[second] -> map[query]count
globalCounts   -> map[query]count
```

Когда приходит событие, сервис увеличивает счетчик в bucket-е и общий счетчик в `globalCounts`. Когда bucket устаревает, его значения вычитаются из `globalCounts`. Благодаря этому не нужно каждый раз заново проходить по всем событиям за 5 минут.

`GET /trends` работает иначе: он не сортирует `globalCounts` на каждый запрос. Раз в секунду background worker строит отсортированный snapshot, а API просто берет из него первые `N` элементов. Это главный компромисс в пользу высокой нагрузки на чтение.

## Антинакрутка

Базовое правило простое:

- для пары `client_id + normalized_query` учитывается не больше одного события за 30 секунд
- `client_id` берется из `user_id`, если он есть, иначе из `session_id`
- если нет ни `user_id`, ни `session_id`, событие учитывается без персонального rate-limit

Это не полноценный antifraud. Но такой механизм уже защищает виджет от самого грубого сценария, когда один клиент много раз подряд отправляет один и тот же запрос.

## Компромиссы

В решении есть несколько осознанных trade-offs:

- Данные хранятся in-memory. Это быстро, но при рестарте текущий топ теряется
- Snapshot обновляется периодически, поэтому топ может отставать примерно на `SNAPSHOT_INTERVAL`
- Stop-list тоже in-memory. Для production-версии я бы вынес его в устойчивое хранилище
- Горизонтальное масштабирование потребует отдельного решения: например, партиционирования по query или общего слоя агрегации
- Текущая защита от накруток базовая, без полноценного antifraud scoring
- Нормализация строк простая и не объединяет похожие по смыслу запросы
- `event_id` есть в контракте, но строгая deduplication в отдельном хранилище не добавлена. Для production-версии ее стоит добавить, если Kafka consumer работает в at-least-once режиме и дубли критичны

## Конфигурация

Сервис конфигурируется через переменные окружения.

| Name | Default |
| --- | --- |
| `HTTP_ADDR` | `:8080` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `search-events` |
| `KAFKA_GROUP_ID` | `trends-service` |
| `WINDOW` | `5m` |
| `FUTURE_SKEW` | `10s` |
| `ABUSE_INTERVAL` | `30s` |
| `SNAPSHOT_INTERVAL` | `1s` |
| `MAX_TOP_ITEMS` | `100` |

## Тесты И Benchmarks

Unit-тесты:

```bash
go test ./...
```

Benchmarks ключевой доменной логики:

```bash
go test -bench=. -benchmem ./internal/trends
```

На моей локальной проверке получились такие ориентировочные значения:

```text
BenchmarkAggregatorAdd-12            3501 ns/op
BenchmarkSnapshotStoreRebuild-12     1757060 ns/op
BenchmarkSnapshotStoreGet-12         72.34 ns/op
```

Самое важное здесь не абсолютные цифры, а порядок: чтение готового snapshot очень дешевое, а тяжелая работа вынесена из read path.

## Нагрузочное Тестирование

Для HTTP-нагрузки можно использовать `hey`.

Установка:

```bash
go install github.com/rakyll/hey@latest
```

PowerShell:

```powershell
.\scripts\load\hey-trends.ps1 -Connections 200 -Duration 30s
```

Linux/macOS:

```bash
CONNECTIONS=200 DURATION=30s ./scripts/load/hey-trends.sh
```

Эквивалентная команда без скрипта:

```bash
hey -z 30s -c 200 "http://localhost:8080/trends?limit=10"
```

## Локальный Запуск Без Docker

Если Kafka уже запущена отдельно:

```bash
go run ./cmd/trends-service
```

По умолчанию сервис ожидает Kafka на `localhost:9092` и topic `search-events`.

## Что Я Бы Добавил Дальше

Если развивать сервис дальше, первые улучшения были бы такими:

- persistence для stop-list
- deduplication по `event_id` с ограниченным TTL
- более строгий readiness check Kafka consumer-а
- graceful деградация при временной недоступности Kafka
- отдельные нагрузочные профили для write path и read path
- распределенная агрегация для нескольких инстансов сервиса
