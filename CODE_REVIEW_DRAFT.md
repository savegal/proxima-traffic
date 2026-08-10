# Черновик код-ревью Proxima Traffic

Статус: в работе. Этот файл пополняется по мере ручной проверки. Исправлены и покрыты регрессиями: CR-001–005, CR-007–015, CR-016–018, CR-019–024 и CR-025. CR-024 теперь использует lease и retry; после падения ровно после внешней отправки Bot API остаётся честная гарантия at-least-once. CR-015 теперь использует единый ограниченный постраничный контракт для всех крупных API-списков и устраняет N+1 в `/campaigns`. Остальные пункты ниже требуют отдельного продуктового решения либо более крупного изменения контракта/инфраструктуры.

## Подтверждённые находки

### CR-001 — P1: обязательный LIVE-preflight отключён в основном запуске через Telegram-бота

- **Где:** `src/proxima_traffic/bot_main.py`, создание `RunQueue` (около строк 117–134).
- **Проблема:** `Settings.preflight_required_live` по умолчанию `True`, но `bot_main.py` не передаёт в `RunQueue` ни `preflight_required`, ни `preflight_max_age_seconds`. Конструктор `RunQueue` использует значение по умолчанию `preflight_required=False` (`src/proxima_traffic/services.py`, около строк 3000–3037). Поэтому при штатном запуске `proxima-bot` LIVE-кампания ставится в очередь без свежей preflight-карты.
- **Контраст:** API-сборка в `src/proxima_traffic/main.py` передаёт `preflight_required=(settings.preflight_required_live and not settings.dry_run)`.
- **Последствие:** возможна реальная отправка в чат без проверки членства, права писать и совместимости медиа, хотя настройка и пользовательский контракт требуют блокировать такой запуск.
- **Рекомендация:** вынести сборку зависимостей в единую фабрику и покрыть интеграционным тестом именно путь `bot_main.main` в LIVE-режиме.

### CR-002 — P1: проверки @SpamBot отсутствуют при штатном запуске control-бота

- **Где:** `src/proxima_traffic/bot_main.py`, около строк 83–95 и 117–182.
- **Проблема:** `bot_main.py` не создаёт `SpamBotCheckService` и не передаёт его в `PreflightService`/`CampaignRunner`; также не запускает `SpamBotMaintenanceWorker`. В API-сборке все три действия выполняются в `src/proxima_traffic/main.py`, около строк 39–53 и 111–144.
- **Последствие:** при запуске через основной CLI `proxima-bot` ограничения аккаунта, обнаруживаемые @SpamBot, не проверяются ни перед preflight, ни после `peer_flood`/`account_restricted`; не выполняется и фоновое обновление статуса. Это обходит предусмотренную защиту аккаунтов.
- **Рекомендация:** использовать ту же единую фабрику сервисов, что и API, и добавить тест на состав зависимостей обоих entrypoint-ов.

### CR-003 — P1: LIVE-режим допускает известный ключ шифрования и известный API-ключ, если не выставлен `environment=production`

- **Где:** `src/proxima_traffic/config.py`, около строк 15, 123–140 и 161–178; `src/proxima_traffic/proxies.py`, около строк 203–212.
- **Проблема:** по умолчанию `environment="development"` и `admin_api_key="local-development-only-change-me"`. Запрет этого значения и обязательность выделенных ключей находятся только внутри ветки `environment == "production"`; значение environment также не нормализуется через `strip()`, поэтому даже `"production "` обходит ветку. При этом LIVE допускается в любом environment, если заданы `dry_run=false` и `live_execution_armed=true`. Сессии аккаунтов и прокси-секреты при отсутствии своих ключей шифруются ключом, производным от известного default API key.
- **Последствие:** ошибочная production-конфигурация без `PROXIMA_ENVIRONMENT=production` оставляет административный API и данные сессий предсказуемо защищёнными/доступными; копию SQLite можно расшифровать известным ключом.
- **Рекомендация:** запретить `dry_run=false` с дефолтным API key независимо от environment; для LIVE всегда требовать отдельные непустые `PROXIMA_ACCOUNT_ENCRYPTION_KEY` и `PROXIMA_PROXY_ENCRYPTION_KEY`.

### CR-004 — P1: по умолчанию LIVE-транспорт допускает прямое подключение без прокси

- **Где:** `src/proxima_traffic/config.py`, около строки 28; `src/proxima_traffic/live_gateway.py`, около строк 926–944; `src/proxima_traffic/accounts.py`, около строк 890–906.
- **Проблема:** `account_require_proxy` имеет значение по умолчанию `False`. Если назначенный прокси отсутствует или нездоров, `TelethonGateway._resolve_proxy()` пишет warning и возвращает `None`; клиент Telethon продолжает работу напрямую. Аналогичный fallback есть в операциях с аккаунтом.
- **Последствие:** IP реального оператора/сервера может быть раскрыт Telegram, а отправка и проверки продолжатся без ожидаемой изоляции. Это противоречит заявленному fail-closed поведению и описанию обязательного account slot в документации.
- **Рекомендация:** сделать прокси обязательным для LIVE (или как минимум по умолчанию) и разрешать direct mode только отдельным явно опасным флагом для разработки.

### CR-005 — P1: единичная инфраструктурная ошибка может остановить критический фоновый worker без перезапуска

- **Где:** `src/proxima_traffic/services.py`, `QueueWorker._worker_loop()` около строк 4338–4349, `RunQueue.claim_next()` около строк 3715–3750; такой же паттерн есть в `src/proxima_traffic/reactions.py` (`run_forever`), `src/proxima_traffic/participant_reactions.py` (`run_forever`) и `src/proxima_traffic/notifications.py` (`run_forever`).
- **Проблема:** `process_next()` вызывает `claim_next()` до своего блока `try`; ошибки SQLite/SQLAlchemy при claim/recovery не перехватываются в `_worker_loop`. Они завершают один worker-task, а `asyncio.gather()` в `run_forever()` завершает supervisor с исключением. У reaction/participant/notifier аналогично не защищены операции claim/scan/recover/получения уведомлений. Entry point не наблюдает и не перезапускает упавшую задачу.
- **Последствие:** transient `database is locked`, I/O error или краткий сбой соединения прекращает обработку очереди, реакций, participant reactions либо уведомлений, хотя бот/API остаётся поднятым. Риск заметен при нескольких параллельных SQLite-писателях и `busy_timeout=5000`.
- **Рекомендация:** оборачивать весь проход каждого worker-loop в обработчик исключений с логированием и bounded backoff; отдельно наблюдать и перезапускать критические фоновые задачи. Паттерн `DelayedDeliveryMonitor` и `ChannelCommentService` уже показывает ожидаемый уровень защиты.

### CR-006 — P2: зависимости проекта формально неразрешимы штатным установщиком

- **Где:** `pyproject.toml`, зависимость `opentele==1.15.1` и комментарий сразу после списка зависимостей.
- **Проверка:** `python -m pip check` в текущем окружении возвращает: `opentele 1.15.1 requires tgcrypto, which is not installed`.
- **Проблема:** проект рассчитывает на локальный shim `cryptg` и вручную предлагает `pip install opentele --no-deps`, однако эта операция не выражена в metadata пакета. Обычный `pip install .` будет пытаться установить `tgcrypto`, а уже установленная среда считается битой для pip.
- **Последствие:** CI/CD, новая Windows-машина или любой обычный менеджер зависимостей могут не установить проект либо пометить окружение неконсистентным.
- **Рекомендация:** заменить/форкнуть зависимость с корректными metadata, оформить extras/constraints и задокументировать воспроизводимую команду установки, не требующую ручного нарушения графа зависимостей.

### CR-007 — P2: путь к рабочей БД по умолчанию зависит от текущего каталога запуска

- **Где:** `src/proxima_traffic/config.py`, `database_url = "sqlite+aiosqlite:///./data/proxima.db"` (около строки 15); `src/proxima_traffic/db.py`, создание родительского каталога (около строк 10–16).
- **Проблема:** entry point `proxima-bot` не фиксирует рабочий каталог. При запуске установленной команды из другого каталога относительный URL создаёт новую БД `./data/proxima.db` рядом с текущим shell, а не использует ожидаемую БД приложения.
- **Последствие:** оператор может незаметно получить пустую БД, потерять видимость очереди/аккаунтов и начать работу с другой копией данных. Та же проблема затрагивает относительные media/backups paths.
- **Рекомендация:** хранить application data в однозначном пользовательском каталоге либо требовать абсолютный `PROXIMA_DATABASE_URL`; на старте логировать и явно подтверждать resolved path.

### CR-008 — P2: API молча скрывает target lists после первых 500 записей

- **Где:** `src/proxima_traffic/api.py`, `GET /api/v1/target-lists`, около строк 586–598.
- **Проблема:** handler всегда вызывает `repository.list_target_lists(0, 500)` и возвращает только `page.items`; параметры страницы и `total` отсутствуют в API-контракте. Репозиторий поддерживает pagination, но наружу она не выведена.
- **Последствие:** при 501+ аудиториях оставшиеся списки невозможно увидеть или выбрать через API, и клиент получает корректный HTTP 200 без признака неполного ответа.
- **Рекомендация:** добавить `page`/`page_size` и метаданные pagination либо сделать лимит явным в ответе; написать тест на 501-ю запись.

### CR-009 — P2: конфигурация принимает нулевой и отрицательный retry backoff

- **Где:** `src/proxima_traffic/config.py`, около строк 87–88 и 294–297; расчёт задержки — `src/proxima_traffic/services.py`, `RunQueue.mark_step_error()` около строк 3790–3812.
- **Проверка:** `Settings(_env_file=None, worker_retry_base_seconds=0, worker_retry_max_seconds=0)`, а также значения `(-1, 0)` и `(-10, -1)`, успешно проходят валидацию.
- **Проблема:** валидатор проверяет только `max >= base`, но не положительность обоих чисел. При отрицательном base временная ошибка назначает run в прошлое, а при нуле — без паузы.
- **Последствие:** ошибочная переменная окружения превращает временную Telegram/сеть/БД-ошибку в быстрый цикл повторов до исчерпания попыток; это повышает риск flood/дублирующих внешних действий и лишает очередь ожидаемого backoff.
- **Рекомендация:** требовать `worker_retry_base_seconds >= 1` и `worker_retry_max_seconds >= worker_retry_base_seconds`; добавить параметризованный тест для 0 и отрицательных значений.

### CR-010 — P2: форматированный HTML-контент валидируется по markup, а не по фактическому тексту Telegram

- **Где:** `src/proxima_traffic/content.py`, `_validate_variant()` около строк 241–268; отправка — `src/proxima_traffic/live_gateway.py`, около строк 1705–1712 и 1812–1818.
- **Проверка:** `new_content_variant(name="test", text="ok", html_text="x" * 5000)` успешно создаёт вариант, хотя фактический текст сообщения имеет 5 000 символов и превышает лимит Telegram 4 096.
- **Проблема:** для `html_text` установлен лишь raw-лимит 16 384 символа. Перед отправкой Telethon превращает HTML в `message_text, entities = html.parse(html_text)`, но длина получившегося `message_text` не проверяется. Проверяется только отдельное поле `text`, которое через API может не совпадать с HTML.
- **Последствие:** кампания проходит сохранение/очередь и падает только при LIVE-отправке как невалидное Telegram-сообщение; для медиа аналогичен лимит caption в 1 024 символа.
- **Рекомендация:** при создании/изменении варианта разбирать HTML тем же парсером и валидировать длину parsed text (4 096 или 1 024 с media); запрещать malformed HTML до постановки в очередь.

### CR-011 — P1: comment/reaction workers используют lease без heartbeat при timeout шага больше lease

- **Где:** claim в `src/proxima_traffic/channel_comments.py` около строк 585–607, `src/proxima_traffic/reactions.py` около строк 136–158 и `src/proxima_traffic/participant_reactions.py` около строк 480–503; recovery просроченных lease — в тех же модулях. Для сравнения, heartbeat реализован только у основной очереди в `src/proxima_traffic/services.py` около строк 4262–4368.
- **Проблема:** фоновым задачам выдаётся `lease_until = now + worker_lease_seconds`, но во время сетевой операции он не продлевается. При дефолтах `worker_lease_seconds=30`, `worker_step_timeout_seconds=120`: корректно работающий запрос, занявший больше 30 секунд, может быть возвращён в очередь другим процессом и взят повторно, пока первоначальный запрос всё ещё выполняется.
- **Последствие:** ломается fence для внешних действий: возможны два параллельных send/edit/reaction для одной durable-задачи, либо устаревший worker выполнит Telegram-действие, но уже не сможет зафиксировать результат. Stable `random_id` уменьшает риск дубля для части отправок, но не заменяет lease heartbeat и не покрывает все действия/состояния.
- **Рекомендация:** добавить heartbeat с token fencing для всех durable workers или инвариантно сделать step timeout меньше lease (первое предпочтительнее); покрыть тестом с искусственно долгим gateway-вызовом и вторым worker.

### CR-012 — P2: FastAPI entry point игнорирует настроенный shutdown grace period

- **Где:** `src/proxima_traffic/main.py`, блок `lifespan` около строк 146–170; для сравнения `src/proxima_traffic/bot_main.py`, `_finish_background_task()` и вызовы около строк 29–51 и 211–250.
- **Проблема:** API lifespan после `stop()` делает прямой `await` каждого фонового task. `Settings.shutdown_grace_seconds` нигде не используется в этом entry point, хотя в control-боте реализован bounded wait с последующим cancel.
- **Последствие:** остановка uvicorn/API может ждать зависший или медленный Telegram/SQLite worker неограниченно (либо дольше операционного grace period), а последующее закрытие gateway и БД не будет выполнено до его завершения.
- **Рекомендация:** переиспользовать `_finish_background_task` или общий lifecycle helper в обоих entry point-ах, чтобы timeout/cancel/закрытие ресурсов были идентичны.

### CR-013 — P2: snapshot состава запуска противоречит повторной проверке текущего списка целей

- **Где:** создание `CampaignRun` из списка — `src/proxima_traffic/services.py`, `RunQueue.enqueue_campaign()` около строк 3050–3470; повторная проверка — `CampaignRunner._load_context()` около строк 309–318. Контракт snapshot описан в `ARCHITECTURE.md` в разделе модели данных.
- **Проблема:** enqueue сохраняет `target_id` в отдельных run и тем самым фиксирует состав партии. Но непосредственно перед отправкой worker требует, чтобы `TargetListItem` по-прежнему существовал и имел `enabled=True`. После удаления/отзыва цели из списка уже поставленная задача получает `ConflictError("target is disabled in this audience list")`.
- **Последствие:** изменение аудитории меняет поведение уже созданной партии вопреки заявленному snapshot-контракту. Задача помечается как `FAILED/preflight_failed`, а не как преднамеренно `CANCELLED`/`SKIPPED`; отчёты смешивают операторскую отмену с реальной ошибкой доставки.
- **Рекомендация:** выбрать и явно закрепить одну семантику: либо состав launch immutable и убрать проверку list item из worker, либо считать отзыв целевой отменой и массово переводить затронутые queued/running runs в отдельный terminal status/reason. Добавить тест на отключение target после enqueue.

### CR-014 — P1: параллельные entry point-ы не имеют межпроцессной блокировки Telegram-аккаунта

- **Где:** `src/proxima_traffic/main.py`, lifespan около строк 124–145, и `src/proxima_traffic/bot_main.py`, около строк 160–183, запускают полный набор workers; локи аккаунта определены как `asyncio.Lock` в `src/proxima_traffic/services.py`, около строк 175–232, и `src/proxima_traffic/live_gateway.py`, около строк 78–82 и 885–891.
- **Проблема:** и API, и control-бот по умолчанию стартуют отправку, комментарии, реакции и другие фоновые задачи. При одновременном запуске двух процессов каждый создаёт собственные `CampaignRunner`, `TelethonGateway`, кэш client-ов и `asyncio.Lock`. Эти объекты не разделяются между процессами; durable-очереди защищают лишь конкретные типы задач, но не являются общим lock-ом на Telegram-аккаунт для всех операций.
- **Последствие:** один Telegram session/auth key может использоваться двумя Telethon-подключениями одновременно, а pacing и cooldown обходятся между процессами. Это приводит к гонкам профиля/отправки, непредсказуемым Telegram-ошибкам и повышает риск flood/restriction. Особенно вероятно при запуске API рядом с ботом либо с несколькими uvicorn workers — оба сценария разрешены текущим entry point-ом.
- **Рекомендация:** разделить роли (только один выделенный worker-процесс, API без workers по умолчанию) и/или ввести DB/Redis advisory lock с lease/fencing на `account_id`, используемый перед любым Telegram-вызовом. Явно запретить несколько worker-процессов в конфигурации и покрыть интеграционным тестом два независимых runner-а на одном account.

### CR-015 — P2: несколько API-списков неограниченны; `/campaigns` дополнительно выполняет N+1 запрос

- **Статус исправления:** закрыто. `GET /accounts`, `/targets`, `/target-lists`, `/templates` и `/campaigns` принимают `page` и `page_size` (1–500) и возвращают единый ответ `{items, total, page, page_size}`. `/campaigns` загружает sender-ов одной выборкой только для кампаний текущей страницы. Контракт и переход на вторую страницу покрыты API-тестами.

- **Где:** `src/proxima_traffic/api.py`: `GET /accounts` около строк 460–462, `GET /targets` около 535–543, `GET /templates` около 663–665 и `GET /campaigns` около 741–744. `_campaign_read()` около строк 1226–1240 отдельно запрашивает sender-ов для каждой broadcast-кампании.
- **Проблема:** endpoints возвращают всю таблицу без `limit`/pagination. У `/campaigns` это ещё и один SQL-запрос на список плюс отдельный запрос `CampaignAccount` на каждую кампанию. Для целевых списков ранее введён скрытый лимит 500 (CR-008), но здесь лимита нет вовсе.
- **Последствие:** с ростом базы один административный GET удерживает в памяти большой ORM-result и формирует крупный JSON-ответ; `/campaigns` делает O(N) round-trip-ов к SQLite. Это может блокировать единственное соединение SQLite и делает UI/API нестабильным именно в сценарии, для которого система хранит большие аудитории.
- **Рекомендация:** ввести единый paginated response (`items`, `total`, `offset`/`limit`) для всех коллекций и ограничить размер страницы. Для кампаний загрузить `CampaignAccount` одним запросом с группировкой или использовать select-in eager loading; добавить тесты на limit и на число запросов.

### CR-016 — P2: одна неотправляемая нотификация навсегда блокирует все последующие

- **Где:** `src/proxima_traffic/notifications.py`, `RunNotifier.process_pending()` около строк 29–61.
- **Проблема:** terminal runs выбираются по возрастанию id и только после успешной отправки получают `notified_at`. Если `_sender()` или `_render()` для самого старого run выбрасывает исключение, код делает `break`. На следующем poll тот же run снова оказывается первым, поэтому до остальных не дойдёт никогда.
- **Последствие:** недоступный log chat, временная ошибка Bot API или один проблемный run превращают очередь оповещений в head-of-line block. Оператор не получает уведомления обо всех более новых ошибках/успехах, хотя основной worker продолжает работать.
- **Рекомендация:** хранить retry metadata/`next_notification_at` и ошибку нотификации отдельно, увеличивать backoff только для проблемного run, но продолжать обработку следующих. Добавить max attempts/dead-letter alert и тест, где первая отправка падает, а вторая всё равно доставляется.

### CR-017 — P2: `proxima-doctor` может сообщить «healthy» для неполной и несовместимой схемы

- **Где:** `src/proxima_traffic/maintenance.py`, константы `REQUIRED_TABLES`/`REQUIRED_COLUMNS` около строк 13–86 и `inspect_sqlite()` около строк 142–174.
- **Проверка:** сравнение `Base.metadata.tables` с `REQUIRED_TABLES` показывает не проверяемые таблицы: `campaign_accounts`, `channel_comment_rules`, `channel_comment_runs`, `reaction_tasks`, `participant_reaction_rules`, `participant_reaction_target_states`, `participant_reaction_tasks`. В частности, `campaign_accounts` нужен текущему broadcast-коду, а последние шесть появились в новых миграциях.
- **Проблема:** команда проверяет только небольшое статическое подмножество таблиц и столбцов и вообще не сопоставляет `alembic_version` с head. Старая БД способна пройти integrity/FK check и получить `"healthy": true`, хотя при первом обращении к новой функции приложение упадёт с `no such table`/`no such column`.
- **Последствие:** перед обновлением или восстановлением backup оператор получает ложный green status и запускает worker с несовместимой схемой. Диагностическая команда не выполняет свою основную защитную функцию.
- **Рекомендация:** проверять все таблицы/критичные столбцы из `Base.metadata` либо сравнивать фактическую Alembic revision с script head; в отчёте отдельно маркировать schema mismatch. Добавить regression-test для БД на ревизии до 0034 и для отсутствующего `campaign_accounts`.

### CR-018 — P3: медиафайлы становятся неубираемыми orphan-ами

- **Где:** запись файла — `src/proxima_traffic/content.py`, `store_media()` около строк 401–433; удаление медиа из варианта — `src/proxima_traffic/bot/repository.py`, `remove_campaign_variant_media()` около строк 2616–2654; удаление варианта — около строк 2693–2718.
- **Проблема:** `store_media()` создаёт новый UUID-файл в `media_storage_path`. При удалении файла из альбома, удалении варианта или удалении кампании из БД убирается лишь JSON-ссылка; поиск по проекту показывает единственный `unlink()` — очистку временного файла сразу после записи. Сборщика неиспользуемых файлов нет.
- **Последствие:** каждое исправление медиа и отменённая кампания оставляют на диске потенциально крупный файл навсегда. На длительно работающем инстансе это незаметно исчерпывает storage и может остановить SQLite/логирование вместе с доставкой.
- **Рекомендация:** после успешного DB commit удалять файл только если он не нужен ни content-variant-ам, ни snapshot-ам исторических runs; либо добавить явную команду безопасного GC с dry-run и age grace period. Нужны тесты на удаление из неиспользованной кампании и на сохранение файла, на который ещё ссылается run snapshot.

### CR-019 — P1: PATCH кампании меняет отображаемый текст, но при наличии вариантов worker отправляет старый

- **Где:** `src/proxima_traffic/api.py`, `update_campaign()` около строк 771–840; выбор контента при enqueue — `src/proxima_traffic/services.py`, `_campaign_content_variants()` около строк 1896–1912 и вызов около 3113–3119. Для сравнения control-бот синхронизирует JSON при изменении основного текста в `src/proxima_traffic/bot/repository.py`, около строк 2299–2329.
- **Проверка:** при `campaign.primary_text = "text saved by API PATCH"` и `content_variants_json` с вариантом `"text actually selected"`, `CampaignRunner._campaign_content_variants(campaign)[0].text` возвращает `"text actually selected"`.
- **Проблема:** API PATCH присваивает `primary_text`/`primary_html` обычным полям Campaign, но не обновляет `content_variants_json`. Если набор вариантов уже существует, `parse_content_variants()` использует JSON и игнорирует fallback-поля. В ответе API и audit будет новый primary text, однако следующий enqueue зафиксирует и отправит предыдущий вариант.
- **Последствие:** оператор получает подтверждение изменения, но публикует устаревший или неверный текст. Это особенно опасно для правок ссылки, условий и срочных остановок контента.
- **Рекомендация:** сделать один источник истины. В API либо запрещать PATCH базовых content-полей при variants, либо, как control-бот, атомарно обновлять выбранный variant и синхронизировать denormalized поля. Добавить тест: создать варианты, PATCH через HTTP, enqueue, затем проверить фактически отправленный текст.

### CR-020 — P1: тот же разрыв источников истины есть у диалоговых цепочек

- **Где:** `src/proxima_traffic/api.py`, `update_campaign()` около строк 771–820; `src/proxima_traffic/services.py`, `_dialogue_steps()` около строк 2206–2227 и snapshot при enqueue около 3076–3090.
- **Проблема:** PATCH с `dialogue_steps` корректно синхронизирует плоские поля и `dialogue_steps_json`, но PATCH только `primary_text`, `reply_text` или `reply_delay_seconds` меняет лишь плоские поля. При существующем `dialogue_steps_json` parser всегда выбирает JSON и игнорирует эти новые значения.
- **Последствие:** API возвращает обновлённую кампанию, но запуск использует прежнюю цепочку, текст и задержки. Это особенно опасно для срочной корректировки уже настроенного диалога.
- **Рекомендация:** для кампании с `dialogue_steps_json` принимать изменения цепочки только целиком через `dialogue_steps`, либо преобразовывать допустимый partial PATCH в новый валидный JSON. Добавить тесты на PATCH каждого базового поля после создания многошагового dialogue.

### CR-021 — P2: кнопка «пауза кампании» превращает уже поставленные задачи в ошибки и не возобновляет их

- **Где:** переключатель — `src/proxima_traffic/bot/repository.py`, `toggle_campaign()` около строк 2789–2805; UI сообщает «Сценарий на паузе» в `src/proxima_traffic/bot/handlers.py`, около 8227–8236; worker отклоняет disabled campaign в `src/proxima_traffic/services.py`, `_validate_config()` около 2839–2845.
- **Проблема:** toggle меняет только `Campaign.enabled`. Очередные runs остаются `QUEUED`; когда worker их берёт, `campaign is disabled` ловится как `ConflictError` и `QueueWorker` помечает run постоянным `FAILED/preflight_failed`. Обратное включение кампании эти задачи не возвращает.
- **Последствие:** «пауза» не ставит рассылку на паузу, а уничтожает остаток запуска с ложными failed-результатами. Включить кампанию обратно недостаточно — оператору приходится создавать новый запуск и терять исходную историю/расписание.
- **Рекомендация:** определить явную семантику: при pause не claim-ить runs и возобновлять их после enable, либо при stop массово ставить `CANCELLED` с причиной `campaign_paused`. Не маркировать операторское действие как технический preflight failure; добавить тест toggle во время очереди и последующий resume.

### CR-022 — P2: `/ready` и `/api/v1/worker` не видят падение большинства фоновых workers

- **Где:** `src/proxima_traffic/main.py`, запуск comment/reaction/participant/delivery/spambot workers около строк 124–145, а `ready()` около 197–207; `src/proxima_traffic/api.py`, `GET /api/v1/worker` около 166–206.
- **Проблема:** readiness считает только `database.ping()` и `queue_worker.snapshot().running`. Статусы `ChannelCommentService`, `ReactionWorker`, `ParticipantReactionService`, `DelayedDeliveryMonitor`, `SpamBotMaintenanceWorker` и `RunNotifier` нигде не публикуются и не входят в health-check. У части этих циклов исключение из claim/recovery вообще завершает task (CR-005).
- **Последствие:** orchestration и оператор видят `200 ready`/`worker: true`, хотя реакции, комментарии, delayed verification, SpamBot-защита или уведомления уже давно не выполняются. Автоматического restart и достоверного alert нет.
- **Рекомендация:** иметь единый supervisor с состоянием/last_error для каждого background task, включить обязательные сервисы в readiness и возвращать degraded/not-ready при аварии. Добавить тест, который искусственно завершает комментарийный/реакционный task и проверяет health response.

### CR-023 — P1: у форматированного контента нет единого тела сообщения между API, dry-run и LIVE

- **Где:** схема разрешает независимые поля `primary_text`/`primary_html` в `src/proxima_traffic/schemas.py`, `CampaignCreate`/`CampaignUpdate`; LIVE transport в `src/proxima_traffic/live_gateway.py`, `_send()` около строк 1690–1730, заменяет `text` на результат `html.parse(html_text)`; `DryRunGateway.send_content()` в `src/proxima_traffic/gateways.py`, около 435–490, напротив, удаляет `html_text` и симулирует отправку `text`.
- **Проверка:** `CampaignCreate(primary_text="checked and shown by dry-run", primary_html="<b>actually sent live</b>", ...)` проходит валидацию. Telethon разбирает HTML в `"actually sent live"`; LIVE отправит его, а dry-run и часть worker-логики работают с первым текстом.
- **Проблема:** API не проверяет, что plain text и разобранный HTML эквивалентны, и нет выбранного canonical payload. Более того, `_send()` при `delivery_unconfirmed` ищет уже отправленное сообщение по исходному `text`, хотя в Telegram ушёл разобранный HTML-текст — recovery не находит его. Стабильный `random_id` обычно предотвращает второй пост, но уже принятая Telegram публикация останется нераспознанной и retry исчерпает попытки.
- **Последствие:** preview/test и LIVE расходятся; аудит/ActionLog может хранить не то, что увидел получатель. В редком сетевом сбое уже доставленный HTML-пост может быть помечен как failed, что подталкивает оператора к ручной повторной отправке.
- **Рекомендация:** принимать только одно canonical поле (например HTML) и получать plain text серверным parser-ом либо требовать их точного совпадения после parse. Передавать parsed text в duplicate recovery; сделать dry-run тем же parser-ом. Добавить тесты на несовпадающую пару и на recovery форматированного сообщения.

### CR-024 — P2: завершённый запуск может дать несколько одинаковых уведомлений

- **Где:** `src/proxima_traffic/notifications.py`, `RunNotifier.process_pending()` около строк 29–61.
- **Проблема:** обработчик без блокировки выбирает terminal-runs с `notified_at IS NULL`, вызывает внешний `_sender`, а отметку `notified_at` сохраняет только после отправки в другой транзакции. У уведомления нет claim/lease или уникального ключа доставки. Два экземпляра `proxima-bot` поэтому оба читают один run, оба отправляют оператору сообщение и затем по очереди ставят одну и ту же отметку. То же окно возникает при падении после успешного `bot.send_message` до commit.
- **Последствия:** оператор получает дубли итоговых уведомлений; при масштабировании или перезапуске число повторов не связано с числом попыток основного run.
- **Рекомендация:** ввести durable claim с lease/fencing token либо outbox со статусами `claimed/sent` и retry/backoff; отправлять должен только владелец claim. Для строгой защиты от повтора в окне crash-after-send нужен ещё внешний идемпотентный ключ либо явная гарантия at-least-once в интерфейсе.

### CR-025 — P1: чистая установка не проходит миграцию `0036`

- **Где:** `migrations/versions/0035_campaign_reactions.py` и `migrations/versions/0036_reaction_task_message_uniqueness.py`.
- **Проверка:** `alembic upgrade head` на пустой SQLite-БД останавливался на revision `0036` с `ValueError: No such constraint: 'uq_reaction_task_reactor'`.
- **Проблема:** revision `0035` уже создаёт `uq_reaction_task_message_reactor`, а следующая revision пытается удалить другое, никогда не созданное ограничение.
- **Последствие:** новая инсталляция и проверка миграций в CI не могут дойти до актуальной схемы БД.
- **Исправление:** `0036` сделана безопасной no-op migration: её целевое состояние уже было создано `0035`.
