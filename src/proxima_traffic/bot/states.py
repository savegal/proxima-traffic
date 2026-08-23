from aiogram.fsm.state import State, StatesGroup


class AccountCreateState(StatesGroup):
    label = State()
    telegram_user_id = State()
    confirm = State()


class AccountImportState(StatesGroup):
    payload = State()


class AccountLabelEditState(StatesGroup):
    label = State()


class AccountNameEditState(StatesGroup):
    first_name = State()
    last_name = State()
    confirm = State()


class AccountBulkNameState(StatesGroup):
    confirm = State()


class AccountBulkAvatarState(StatesGroup):
    photo = State()
    confirm = State()


class AccountAvatarEditState(StatesGroup):
    photo = State()


class AccountAboutEditState(StatesGroup):
    text = State()
    confirm = State()


class AccountAboutBulkState(StatesGroup):
    text = State()
    confirm = State()


class AccountGroupCreateState(StatesGroup):
    name = State()
    description = State()


class AccountGroupEditState(StatesGroup):
    name = State()
    description = State()


class TargetCreateState(StatesGroup):
    chat_ref = State()


class TargetImportState(StatesGroup):
    name = State()
    document = State()
    folder_account = State()


class TargetListRenameState(StatesGroup):
    name = State()


class TargetCooldownState(StatesGroup):
    minutes = State()


class TemplateCreateState(StatesGroup):
    name = State()
    primary_text = State()
    reply_text = State()
    confirm = State()


class TemplateEditState(StatesGroup):
    primary_text = State()
    reply_text = State()
    confirm = State()


class CampaignCreateState(StatesGroup):
    name = State()
    mode = State()
    primary_account = State()
    sender_accounts = State()
    reply_account = State()
    target = State()
    target_search = State()
    template = State()
    primary_text = State()
    reply_text = State()
    reply_delay = State()
    chain_action = State()
    chain_account = State()
    chain_text = State()
    chain_delay = State()
    repeat_mode = State()
    spread_hours = State()
    smart_duration_hours = State()
    smart_no_timer_interval_minutes = State()
    repeat_interval_minutes = State()
    repeat_count = State()
    confirm = State()


class CampaignEditState(StatesGroup):
    target = State()
    target_selection = State()
    search = State()
    senders = State()
    mention_cooldown = State()


class CampaignContentEditState(StatesGroup):
    primary_text = State()


class CampaignVariantState(StatesGroup):
    name = State()
    content = State()
    media = State()
    contact = State()


class CampaignScheduleEditState(StatesGroup):
    mode = State()
    interval_minutes = State()
    repeat_count = State()
    smart_duration_hours = State()
    smart_no_timer_interval_minutes = State()
    advanced_value = State()


class LaunchEditState(StatesGroup):
    primary_text = State()
    no_timer_interval_minutes = State()
    min_messages_between_posts = State()
    visibility_max_wait_minutes = State()
    remaining_duration_hours = State()


class OrderDraftState(StatesGroup):
    action = State()
    name = State()
    text = State()
    sender_accounts = State()
    audience = State()
    audience_targets = State()
    audience_search = State()
    engagement = State()
    mention_cooldown_days = State()
    interval_minutes = State()
    visibility_messages = State()
    activity_check_seconds = State()
    visibility_max_wait_minutes = State()
    duration_hours = State()
    spread_hours = State()


class ListSearchState(StatesGroup):
    query = State()


class ChannelCommentCreateState(StatesGroup):
    name = State()
    channel = State()
    primary_account = State()
    primary_text = State()
    message_edit_final_text = State()
    message_edit_delay = State()
    chain_account = State()
    chain_text = State()
    chain_delay = State()
    initial_delay = State()
    confirm = State()
    replace_primary_account = State()
    mention_cooldown = State()


class ParticipantReactionCreateState(StatesGroup):
    name = State()
    target_list = State()
    accounts = State()
    sample = State()
    emoji = State()
    confirm = State()


class ParticipantReactionEditState(StatesGroup):
    target_list = State()
    accounts = State()
    sample = State()
    emoji = State()
    confirm = State()


class ProxyAddState(StatesGroup):
    url = State()
    label = State()
    group = State()
    rotation_url = State()


class ProxyImportState(StatesGroup):
    group = State()
    document = State()


class SubscriberImportState(StatesGroup):
    document = State()


class DirectBlacklistState(StatesGroup):
    ids = State()


class DirectCampaignCreateState(StatesGroup):
    limit = State()
    content = State()


class SubscriberInviteCreateState(StatesGroup):
    destination = State()
    limit = State()


class ProxyAssignState(StatesGroup):
    assignment_key = State()
    purpose = State()


class ProxyEditState(StatesGroup):
    url = State()


class ProxyRotationState(StatesGroup):
    url = State()


class ProxyMetadataEditState(StatesGroup):
    label = State()
    group = State()


class ProxyAutoAssignState(StatesGroup):
    group = State()
    assignment_key = State()
    purpose = State()


class ProxyGroupCreateState(StatesGroup):
    name = State()
    description = State()
