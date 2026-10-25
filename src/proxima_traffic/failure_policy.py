"""Shared Telegram failure classification for every background workflow.

Telegram failures have different scopes.  A dead authorization belongs to the
whole account, a moderator ban belongs only to one account/chat pair, and a
deleted or globally unavailable chat belongs to the destination.  Keeping the
sets here prevents comments, broadcasts and reactions from making conflicting
decisions for the same RPC error.
"""

from __future__ import annotations

from enum import StrEnum


class FailureScope(StrEnum):
    ACCOUNT_FATAL = "account_fatal"
    ACCOUNT_TEMPORARY = "account_temporary"
    ACCOUNT_CHAT = "account_chat"
    TARGET = "target"
    CONTENT = "content"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"


ACCOUNT_FATAL_CODES = frozenset({"account_dead", "account_unauthorized"})
CAMPAIGN_UNCERTAIN_CODES = frozenset(
    {
        "delivery_unconfirmed",
        "step_timeout",
        "unhandled_execution_error",
        "account_chat_access_lost",
        "cancelled_outcome_unknown",
    }
)
CAMPAIGN_NON_RETRYABLE_CODES = CAMPAIGN_UNCERTAIN_CODES | frozenset({
    "lease_expired", "post_deleted_by_moderation", "album_partial_missing",
})
ACCEPTED_CAMPAIGN_ACTION_CODES = frozenset({
    "post_deleted_by_moderation", "membership_gate_recovered", "album_partial_missing",
})
ACCOUNT_TEMPORARY_CODES = frozenset(
    {
        "account_restricted",
        "peer_flood",
        "flood_wait",
    }
)
ACCOUNT_WIDE_CODES = ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES

ACCOUNT_CHAT_BLOCK_CODES = frozenset(
    {
        # ``USER_BANNED_IN_CHANNEL`` is the only Telegram signal in this group
        # that confirms an actual ban.  The remaining codes mean a route is
        # unusable for now, but must not be presented to an operator as a ban.
        "account_banned_in_chat",
        "account_chat_access_forbidden",
        "account_chat_access_lost",
        "account_missing_post_rights",
        "account_missing_admin_rights",
        "account_send_forbidden",
    }
)

# Durable route failures: another account may still reach this target.  They
# are cleared by a successful preflight or an explicit operator unblock.
ACCOUNT_TARGET_BLOCK_CODES = ACCOUNT_CHAT_BLOCK_CODES | frozenset(
    {
        "account_join_limit",
        "account_chatlist_limit",
        "join_request_pending",
        "target_not_joined",
    }
)

TARGET_FAILURE_CODES = frozenset(
    {
        "account_join_limit",
        "join_request_pending",
        "post_deleted_by_moderation",
        "target_not_joined",
        "target_unavailable",
    }
)

CONTENT_FAILURE_CODES = frozenset(
    {
        "comment_invalid",
        "content_forbidden",
        "premium_emoji_unavailable",
        "reaction_invalid",
        "reaction_unavailable",
        "request_invalid",
    }
)

TRANSIENT_CODES = frozenset(
    {
        "account_pacing",
        "chat_timer",
        "comment_timeout",
        "delivery_unconfirmed",
        "join_throttled",
        "join_completed",
        "network_error",
        "proxy_unavailable",
        "reaction_delivery_unconfirmed",
        "rpc_error",
        "slow_mode",
    }
)

OBSERVED_TARGET_FAILURE_CODES = ACCOUNT_TARGET_BLOCK_CODES | TARGET_FAILURE_CODES

_FAILURE_LABELS = {
    "album_duplicate": "элемент альбома обработан общим комментарием",
    "account_banned_in_chat": "Telegram подтвердил бан аккаунта в этом чате",
    "account_chat_access_forbidden": (
        "Telegram запретил этому аккаунту действие в чате (бан не подтверждён)"
    ),
    "account_chat_access_lost": (
        "после отправки аккаунт потерял доступ к чату (бан не подтверждён)"
    ),
    "account_chatlist_limit": "аккаунт достиг лимита папок Telegram для вступления",
    "account_dead": "сессия или API-данные аккаунта недействительны",
    "account_join_limit": "аккаунт достиг лимита групп и каналов Telegram",
    "account_missing_admin_rights": "аккаунту не хватает требуемых прав в чате",
    "account_missing_post_rights": "аккаунту не выдано право публиковать в канале",
    "account_operation_busy": "аккаунт занят другой операцией",
    "account_pacing": "отправка отложена безопасным лимитом аккаунта",
    "join_completed": "аккаунт вступил в чат; пауза перед публикацией",
    "account_paused": "аккаунт поставлен на паузу",
    "account_restricted": "Telegram отказал в отправке с аккаунта",
    "account_send_forbidden": (
        "этому аккаунту сейчас запрещено писать в чате (бан не подтверждён)"
    ),
    "account_unauthorized": "аккаунт больше не авторизован",
    "cancel_requested": "операция останавливается по запросу",
    "cancelled": "операция отменена",
    "cancelled_before_send": "операция отменена до отправки",
    "cancelled_outcome_unknown": "операция отменена, но результат Telegram неизвестен",
    "capacity_window_exceeded": "аккаунт не успевает отправить до конца active-окна",
    "channel_has_no_posts": "в канале пока нет доступных публикаций",
    "channel_preflight_timeout": "проверка канала не ответила вовремя",
    "channel_required": "выбранный адрес больше не является каналом",
    "channel_scan_timeout": "проверка новых публикаций не ответила вовремя",
    "chat_timer": "в чате ещё действует сохранённый таймер отправки",
    "comment_edit_invalid": "настройки редактирования комментария некорректны",
    "comment_edit_unavailable": "Telegram больше не разрешает редактировать комментарий",
    "comment_expired": "пост уже слишком старый для комментария",
    "comment_invalid": "текст или настройки комментария некорректны",
    "comment_timeout": "отправка комментария не завершилась вовремя",
    "comments_unavailable": "комментарии к публикации отключены или недоступны",
    "comment_thread_changed": "ветка обсуждения изменилась",
    "comment_execution_mode_changed": "режим тестовой / реальной отправки изменился",
    "comment_paid_message_required": "обсуждение требует оплату Stars",
    "comment_sender_requires_premium": "для комментария требуется Telegram Premium",
    "completed_during_cancel": "операция завершилась во время остановки",
    "contact_invalid": "данные контакта некорректны",
    "content_forbidden": "выбранный тип содержимого запрещён в чате",
    "content_invalid": "содержимое сообщения некорректно",
    "delivery_unconfirmed": "результат отправки Telegram не подтверждён",
    "delivery_outcome_unknown": "отправка прервалась; результат Telegram неизвестен",
    "launch_mode_changed": "режим симуляции изменился после создания запуска",
    "database_busy": "подготовка отложена из-за занятости базы данных",
    "telegram_forbidden": "Telegram запретил запрос; причина не распознана",
    "recipient_content_forbidden": "получатель не принимает этот тип сообщения",
    "flood_wait": "Telegram потребовал паузу перед следующей операцией",
    "gateway_error": "Telegram-шлюз вернул ошибку",
    "gateway_permanent": "Telegram окончательно отклонил операцию",
    "gateway_temporary": "Telegram временно не выполнил операцию",
    "global_stop": "операция остановлена общей аварийной остановкой",
    "join_request_pending": "заявка на вступление ожидает одобрения",
    "join_throttled": "вступление отложено безопасным лимитом",
    "invite_already_participant": "человек уже состоит в выбранной цели",
    "invite_admin_manager_rights": "у главного аккаунта нет права назначать администраторов",
    "invite_admin_limit": "в цели достигнут лимит администраторов",
    "invite_admin_fresh_restriction": "Telegram пока запрещает новому администратору менять права",
    "invite_admin_update_unconfirmed": "изменение прав администратора не подтверждено",
    "invite_admin_target_unresolvable": (
        "Telegram не смог найти аккаунт для назначения администратором"
    ),
    "invite_delivery_unconfirmed": "результат приглашения Telegram не подтверждён",
    "invite_destination_full": "в выбранной цели достигнут лимит участников",
    "invite_destination_changed": "адрес теперь ведёт к другому каналу или группе",
    "invite_destination_unverified": "в старом запуске не сохранён ID цели Telegram",
    "invite_destination_invalid": "цель приглашения недействительна или не поддерживается",
    "invite_destination_link_invalid": "приватная ссылка приглашения недействительна или истекла",
    "invite_destination_rights": "у аккаунта нет права приглашать участников в цель",
    "invite_destination_unavailable": "выбранная цель недоступна этому аккаунту",
    "invite_launch_cancelled": "запуск приглашений остановлен",
    "invite_recipient_limit": "у человека достигнут лимит каналов и групп Telegram",
    "invite_recipient_privacy": "настройки человека не разрешают это приглашение",
    "invite_recipient_privacy_premium_pm": (
        "приватность запрещает приглашение; для личной ссылки также требуется Premium"
    ),
    "invite_recipient_contact_required": "приглашение доступно только взаимным контактам",
    "invite_sender_requires_premium": "для приглашения нужен Premium у приглашающего аккаунта",
    "invite_recipient_kicked": "человек исключён из выбранной цели",
    "invite_recipient_segment_changed": "текущий Premium-статус человека не соответствует сегменту",
    "invite_preflight_unavailable": "проверка перед приглашением временно недоступна",
    "invite_request_rejected": "Telegram отказал в добавлении участника",
    "invite_recipient_unavailable": "человека нельзя пригласить по сохранённым данным",
    "invite_recipient_unresolvable": "аккаунт не может безопасно разрешить человека",
    "invite_reservation_lost": "человек уже занят другим запуском приглашений",
    "launch_cancelled": "запуск рассылки отменён",
    "lease_expired": "worker потерял подтверждение выполнения задачи",
    "lease_recovered": "задача восстановлена после прерванного worker",
    "media_unavailable": "медиафайл отсутствует или недоступен",
    "membership_gate_invalid_channel": "защитная проверка чата содержит некорректный канал",
    "membership_gate_recovered": "требование подписки выполнено",
    "membership_gate_retry": "сообщение повторится после выполнения требования подписки",
    "membership_gate_retry_outside_window": "повтор не помещается в срок актуальности",
    "membership_gate_verification_failed": "чат не подтвердил выполнение требования подписки",
    "network_error": "временная сетевая ошибка",
    "participant_reaction_timeout": "авто-реакция не завершилась вовремя",
    "peer_flood": "Telegram ограничил активность аккаунта",
    "premium_emoji_unavailable": "Telegram не принял Premium Emoji в сообщении",
    "post_deleted_by_moderation": "публикация не найдена после отправки",
    "album_partial_missing": "часть отправленного альбома отсутствует",
    "preparation_timeout": "подготовка сообщения не завершилась вовремя",
    "preparation_failed": "сбой подготовки до отправки",
    "primary_account_auto_replaced": "основной аккаунт автоматически заменён",
    "preflight_dialogue_not_ready": "не все аккаунты диалога готовы к этому чату",
    "preflight_account_cooldown": "аккаунт временно на защитной паузе Telegram",
    "preflight_failed": "проверка готовности не пройдена",
    "preflight_independent_not_ready": "этот аккаунт не готов к отправке в этот чат",
    "preflight_no_ready_sender": "нет готового отправителя для этого чата",
    "primary_account_replaced": "основной аккаунт правила был заменён",
    "proxy_unavailable": "не найден рабочий прокси для аккаунта",
    "recipient_identity_mismatch": "username теперь принадлежит другому пользователю",
    "recipient_blacklisted": "получатель находится в глобальном стоп-листе",
    "recipient_opted_out": "получатель находится в списке «не писать»",
    "recipient_paid_message_required": "получатель принимает только платные сообщения",
    "recipient_privacy_restricted": "приватность получателя запрещает новое сообщение",
    "recipient_requires_premium": "получатель принимает новые сообщения только от Premium",
    "recipient_route_blocked": "выбранный аккаунт не может написать получателю",
    "recipient_reservation_lost": "получатель уже занят другим запуском",
    "recipient_unavailable": "получатель удалён или больше недоступен",
    "recipient_unresolvable": "получателя нельзя безопасно разрешить по сохранённым данным",
    "service_already_set": "настройка уже установлена; нового сервисного сообщения нет",
    "service_username_required": "для сервисного действия нужен @username",
    "service_recipient_not_premium": "у получателя больше нет Premium",
    "service_sender_requires_premium": "для этого действия нужен Premium у аккаунта",
    "service_sender_unavailable": "назначенный аккаунт не готов; автозамена запрещена",
    "service_forbidden": "Telegram запретил изменение настройки диалога",
    "service_unsupported": "клиент не поддерживает настройку личного диалога",
    "service_invalid": "неверная настройка сервисного действия",
    "service_notification_unconfirmed": "настройка подтверждена; сервисное сообщение не проверено",
    "reaction_invalid": "выбранная реакция некорректна",
    "reaction_delivery_unconfirmed": ("Telegram принял реакцию, но не подтвердил её отображение"),
    "reaction_timeout": "реакция не завершилась вовремя",
    "reaction_unavailable": (
        "выбранная реакция запрещена в этом сообщении или недоступна аккаунту"
    ),
    "reactions_disabled": "реакции отключены в настройках",
    "reactor_removed_from_rule": "аккаунт удалён из выбранного пула реакций",
    "reactor_unavailable": "нет доступного аккаунта для реакции",
    "repeat_window_elapsed": "окно повторной отправки уже закончилось",
    "request_invalid": "Telegram отклонил параметры запроса",
    "rpc_error": "временная внутренняя ошибка Telegram",
    "rule_archived": "правило перемещено в архив",
    "rule_auto_paused": "правило автоматически остановлено после постоянной ошибки",
    "rule_paused": "правило остановлено",
    "sender_unavailable": "назначенный аккаунт недоступен",
    "sender_refused_in_launch": "аккаунт исключён из запуска после отказа Telegram",
    "slow_mode": "в чате действует медленный режим",
    "spambot_unavailable": "@SpamBot не ответил или недоступен этому аккаунту",
    "step_timeout": "этап отправки не завершился вовремя",
    "target_list_empty": "в списке нет включённых чатов",
    "target_low_quality": "чат временно исключён из-за низкой успешности",
    "target_not_joined": "аккаунт ещё не состоит в чате",
    "target_unavailable": "канал или группа недоступны",
    "unhandled_execution_error": "неожиданная ошибка выполнения",
}

_FAILURE_HINTS = {
    "account_banned_in_chat": (
        "Сервис исключит только эту пару аккаунт–чат и попробует другого отправителя."
    ),
    "account_chat_access_forbidden": (
        "Сервис исключит только эту пару аккаунт–чат; это не считается подтверждённым баном."
    ),
    "account_chat_access_lost": (
        "Проверьте доступ к чату: Telegram не уточнил причину, поэтому бан не подтверждён."
    ),
    "account_chatlist_limit": (
        "Освободите слот папки Telegram либо используйте прямую ссылку на публичный чат."
    ),
    "account_dead": "Перепроверьте или заново импортируйте сессию этого аккаунта.",
    "account_join_limit": "Используйте другой аккаунт или освободите место в списке каналов.",
    "account_missing_admin_rights": "Выдайте аккаунту необходимые права или выберите другого.",
    "account_missing_post_rights": (
        "Выдайте право публиковать в канале или выберите группу для обычной рассылки."
    ),
    "account_operation_busy": "Сервис повторит операцию после освобождения аккаунта.",
    "account_pacing": "Ничего делать не нужно: сервис продолжит автоматически.",
    "account_restricted": (
        "Telegram не сообщил срок снятия ограничения. Проверьте статус аккаунта; "
        "отказ сам по себе не означает обязательное ожидание 24 часов."
    ),
    "account_send_forbidden": (
        "Сервис исключит только эту пару; это может быть режим чата или ограничение прав."
    ),
    "account_unauthorized": "Заново импортируйте сессию аккаунта.",
    "cancelled_outcome_unknown": (
        "Проверьте сообщение в Telegram перед ручным повтором, чтобы не создать дубль."
    ),
    "capacity_window_exceeded": (
        "Увеличьте срок active-рассылки, уменьшите объём либо дождитесь освобождения "
        "лимита аккаунта."
    ),
    "channel_preflight_timeout": "Повторите проверку позже; остальные каналы продолжат работу.",
    "channel_scan_timeout": "Сервис повторит проверку; остальные правила не остановятся.",
    "chat_timer": "Ничего делать не нужно: отправка уже перенесена на разрешённое время.",
    "comment_edit_unavailable": "Проверьте автора комментария и допустимый срок редактирования.",
    "comment_timeout": "Сервис проверит результат и выполнит безопасный повтор.",
    "comments_unavailable": "Проверьте, включено ли обсуждение у публикации канала.",
    "comment_thread_changed": (
        "Продолжение остановлено, чтобы не написать в другую ветку. Проверьте обсуждение канала."
    ),
    "comment_execution_mode_changed": (
        "Начатая цепочка или редактирование остановлены. Новые посты используют текущий режим."
    ),
    "comment_paid_message_required": (
        "Сервис не оплачивает комментарии. Выберите обсуждение без обязательной оплаты."
    ),
    "comment_sender_requires_premium": (
        "Проверьте подписку выбранного аккаунта; автоповтор не выполняется."
    ),
    "content_forbidden": (
        "Измените тип медиа или разрешения чата; текстовая отправка не блокируется."
    ),
    "content_invalid": "Исправьте текст, разметку или вложения перед повтором.",
    "delivery_unconfirmed": (
        "Проверьте диалог или чат отправителя до ручного повтора, чтобы избежать дубля."
    ),
    "launch_mode_changed": "Создайте новый запуск в текущем режиме работы сервиса.",
    "flood_wait": "Сервис соблюдёт паузу Telegram и продолжит автоматически.",
    "join_request_pending": "Дождитесь одобрения заявки или выберите другой доступный чат.",
    "join_throttled": "Ничего делать не нужно: вступление продолжится после безопасной паузы.",
    "invite_delivery_unconfirmed": (
        "Повтор не выполняется автоматически: Telegram мог добавить человека до потери ответа."
    ),
    "invite_admin_manager_rights": (
        "Сделайте главный аккаунт владельцем или дайте ему право «Добавлять администраторов / "
        "Add new admins», затем повторите подготовку прав."
    ),
    "invite_admin_limit": "Уберите ненужных администраторов перед повторной подготовкой.",
    "invite_admin_fresh_restriction": "Дождитесь снятия ограничения Telegram; срок не указан.",
    "invite_admin_update_unconfirmed": (
        "Проверьте фактические права выбранного аккаунта в Telegram."
    ),
    "invite_admin_target_unresolvable": (
        "Обновите @username аккаунта или сделайте его доступным главному аккаунту; одного "
        "числового Telegram ID для назначения обычно недостаточно."
    ),
    "invite_destination_full": "Освободите место в цели или выберите другую цель.",
    "invite_destination_changed": "Проверьте адрес и создайте новый запуск для нужной цели.",
    "invite_destination_unverified": (
        "Создайте новый запуск: сервис заново проверит и закрепит цель."
    ),
    "invite_destination_invalid": "Укажите канал или супергруппу, которыми вы управляете.",
    "invite_destination_link_invalid": (
        "Укажите действующую приватную ссылку и создайте новый запуск."
    ),
    "invite_destination_rights": (
        "Выдайте выбранному аккаунту права администратора с разрешением приглашать участников."
    ),
    "invite_destination_unavailable": (
        "Проверьте ссылку и доступ выбранных аккаунтов; закрытая цель не будет "
        "открываться автоматически."
    ),
    "invite_launch_cancelled": (
        "Новые задания отменены; уже отправленные приглашения останутся в Telegram."
    ),
    "invite_recipient_limit": "Запись пропущена без повторных попыток.",
    "invite_recipient_privacy": (
        "Контакт пропущен. Автоматический повтор через другие аккаунты не выполняется."
    ),
    "invite_recipient_privacy_premium_pm": (
        "Premium нужен для личной ссылки, но не снимает запрет на добавление. Контакт пропущен."
    ),
    "invite_recipient_contact_required": "Контакт пропущен; сервис не меняет контакты аккаунтов.",
    "invite_sender_requires_premium": (
        "Контакт пропущен. Проверьте подписку приглашающего аккаунта перед ручным новым запуском."
    ),
    "invite_recipient_kicked": "Контакт пропущен; сервис не снимает запреты администраторов.",
    "invite_recipient_segment_changed": "Контакт пропущен: сегмент проверен по текущему профилю.",
    "invite_preflight_unavailable": (
        "Приглашение ещё не отправлено; проверка повторится после паузы."
    ),
    "invite_request_rejected": (
        "Участник не добавлен. Технический ответ Telegram сохранён в отчёте."
    ),
    "invite_recipient_unavailable": "Обновите базу подписчиков; запись пропущена безопасно.",
    "invite_recipient_unresolvable": (
        "Нужен актуальный username или уже известный этому аккаунту профиль; обхода нет."
    ),
    "invite_reservation_lost": "Запись не будет приглашена повторно в этом запуске.",
    "media_unavailable": "Загрузите файл заново или удалите его из варианта публикации.",
    "premium_emoji_unavailable": (
        "Проверьте, что Emoji ещё доступен и выбранный аккаунт может его использовать."
    ),
    "reaction_delivery_unconfirmed": (
        "Сервис повторит ту же реакцию: для одного аккаунта это безопасно и не создаст дубль."
    ),
    "reaction_unavailable": (
        "Обычно в чате выключены реакции или выбранный Emoji не разрешён; "
        "публикация уже отправлена и другие чаты продолжат работу."
    ),
    "membership_gate_verification_failed": "Проверьте требования бота-модератора вручную.",
    "network_error": "Сервис повторит операцию автоматически.",
    "post_deleted_by_moderation": "Проверьте правила чата и требования его бота-модератора.",
    "album_partial_missing": (
        "Повтор альбома запрещён, чтобы не дублировать сохранившиеся элементы."
    ),
    "preparation_timeout": "Запрос отправки ещё не начался; подготовка повторится после паузы.",
    "preparation_failed": "Запрос отправки ещё не начался; устраните причину и повторите задачу.",
    "primary_account_auto_replaced": (
        "Новый аккаунт прошёл проверку канала; будущие первые комментарии пойдут от него."
    ),
    "preflight_dialogue_not_ready": "Повторите проверку аккаунтов и чатов перед запуском.",
    "preflight_account_cooldown": (
        "Дождитесь окончания паузы Telegram или выберите другой готовый аккаунт."
    ),
    "preflight_failed": "Откройте «Проверить чаты и аккаунты» и устраните указанные причины.",
    "preflight_independent_not_ready": (
        "Проверьте доступ именно этой пары аккаунт–чат; остальные готовые пары продолжают работу."
    ),
    "preflight_no_ready_sender": "Добавьте рабочий аккаунт или восстановите доступ к чату.",
    "sender_unavailable": (
        "Аккаунт отключён или его сессия недействительна; заново импортируйте tdata "
        "либо исключите его из рассылки."
    ),
    "proxy_unavailable": (
        "Автозамена проверила все включённые прокси, но рабочего варианта не нашла. "
        "Добавьте или включите рабочий HTTP(S)/SOCKS5-прокси."
    ),
    "recipient_identity_mismatch": "Запись пропущена, чтобы сообщение не ушло другому человеку.",
    "delivery_outcome_unknown": (
        "Проверьте диалог отправителя вручную; автоматическая повторная отправка запрещена."
    ),
    "telegram_forbidden": "Сохранён исходный код Telegram; проверьте детали ошибки в запуске.",
    "recipient_content_forbidden": "Для этого получателя нужен другой разрешённый тип сообщения.",
    "database_busy": "Подготовка повторится автоматически, когда база освободится.",
    "recipient_blacklisted": "Запись пропущена без попытки отправки.",
    "recipient_opted_out": "Запись пропущена без попытки отправки.",
    "recipient_paid_message_required": (
        "Получатель в карантине ЛС; сервис не оплачивает Stars. Доступна проверка без отправки."
    ),
    "recipient_privacy_restricted": (
        "Запись пропущена; повтор с этого аккаунта не поможет без изменения приватности."
    ),
    "recipient_requires_premium": "Выберите Premium-аккаунт отправителя либо исключите запись.",
    "recipient_route_blocked": "Сервис попробует другой выбранный аккаунт, если он доступен.",
    "recipient_reservation_lost": "Запись не будет отправлена повторно.",
    "recipient_unavailable": (
        "Получатель в карантине ЛС; импорт не снимает его. "
        "Доступна проверка свежих данных Telegram."
    ),
    "recipient_unresolvable": "Нужен актуальный username или доступ аккаунта к общему чату.",
    "reactor_removed_from_rule": (
        "Задача отменена до отправки: правило теперь использует другой набор аккаунтов."
    ),
    "reactor_unavailable": "Добавьте рабочие аккаунты-реакторы или уменьшите число реакций.",
    "request_invalid": "Проверьте содержимое и настройки операции перед повтором.",
    "rpc_error": "Сервис выполнит ограниченный автоматический повтор.",
    "rule_auto_paused": "Проверьте канал и аккаунты, затем включите правило вручную.",
    "slow_mode": "Отправка автоматически перенесена на разрешённое Telegram время.",
    "spambot_unavailable": "Статус не считается баном; повторите проверку позже.",
    "step_timeout": "Сервис сохранит состояние и не будет бесконтрольно повторять отправку.",
    "target_low_quality": "Проверьте чат и вручную верните его после устранения причины.",
    "target_not_joined": "Разрешите автовступление или укажите действующую invite-ссылку.",
    "target_unavailable": "Проверьте адрес, доступность чата и право аккаунта писать в него.",
    "unhandled_execution_error": "Откройте журнал; задача остановлена без бесконечных повторов.",
}

# A channel-comment rule normally has a fixed route and pauses on permanent
# account/channel or fixed-content failures. Rules that explicitly enable
# sender replacement may fail over before their first comment, after the
# replacement passes preflight. A missing attachment or an unsupported media
# type would fail identically for every new post, so keeping the rule active
# would only produce repeated Telegram errors.
CHANNEL_COMMENT_AUTO_PAUSE_CODES = (
    ACCOUNT_FATAL_CODES
    | ACCOUNT_CHAT_BLOCK_CODES
    | frozenset(
        {
            "account_join_limit",
            "account_paused",
            "channel_required",
            "comment_invalid",
            "comment_paid_message_required",
            "comment_sender_requires_premium",
            "content_forbidden",
            "join_request_pending",
            "media_unavailable",
            "premium_emoji_unavailable",
            "sender_unavailable",
            "target_not_joined",
            "target_unavailable",
        }
    )
)


def failure_code(error: BaseException | str | None, fallback: str = "unknown_error") -> str:
    if isinstance(error, str):
        normalized = error.strip()
    elif error is None:
        normalized = ""
    else:
        normalized = str(getattr(error, "code", None) or "").strip()
    return normalized or fallback


def failure_scope(code: str | None) -> FailureScope:
    normalized = failure_code(code)
    if normalized in ACCOUNT_FATAL_CODES:
        return FailureScope.ACCOUNT_FATAL
    if normalized in ACCOUNT_TEMPORARY_CODES:
        return FailureScope.ACCOUNT_TEMPORARY
    if normalized in ACCOUNT_CHAT_BLOCK_CODES:
        return FailureScope.ACCOUNT_CHAT
    if normalized in TARGET_FAILURE_CODES:
        return FailureScope.TARGET
    if normalized in CONTENT_FAILURE_CODES:
        return FailureScope.CONTENT
    if normalized in TRANSIENT_CODES:
        return FailureScope.TRANSIENT
    return FailureScope.UNKNOWN


def failure_label(code: str | None) -> str:
    normalized = failure_code(code)
    return _FAILURE_LABELS.get(normalized, normalized.replace("_", " "))


def failure_hint(code: str | None) -> str | None:
    """Return a short operator-facing next action without exposing RPC internals."""

    return _FAILURE_HINTS.get(failure_code(code))
