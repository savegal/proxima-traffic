"""Pure parsing primitives for Telegram's official @SpamBot responses.

The response language follows the Telegram client's locale, so this module
never assumes a single language or a date format. A genuinely unrecognised
response is reported as ``UNKNOWN``. A response which explicitly mentions an
account restriction but cannot be classified precisely is instead kept in a
protective restricted state. Actual send errors remain the final authority for
per-chat delivery diagnostics.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum


class SpamBotStatus(StrEnum):
    FREE = "free"
    TEMPORARY_RESTRICTION = "temporary_restriction"
    ACTIVE_RESTRICTION = "active_restriction"
    PERMANENT_RESTRICTION = "permanent_restriction"
    RESTRICTION_UNCLEAR = "restriction_unclear"
    UNKNOWN = "unknown"


class SpamBotRestrictionScope(StrEnum):
    """What @SpamBot explicitly says is restricted.

    ``DIRECT_OUTREACH`` does not mean that publishing in every existing chat is
    allowed.  Telegram's text only confirms the capabilities listed here, so
    callers must keep restricted accounts out of automated campaigns.
    """

    NONE = "none"
    DIRECT_OUTREACH = "direct_outreach"
    DIRECT_OUTREACH_AND_INVITES = "direct_outreach_and_invites"
    UNSPECIFIED = "unspecified"


@dataclass(frozen=True, slots=True)
class SpamBotResult:
    status: SpamBotStatus
    raw_text: str
    restricted_until: datetime | None = None
    parser_signal: str = "unknown"
    button_rows: tuple[tuple[str, ...], ...] = ()
    format_entities: tuple[tuple[str, int, int], ...] = ()
    restriction_scope: SpamBotRestrictionScope = SpamBotRestrictionScope.NONE
    allows_reply_to_inbound: bool | None = None

    @property
    def is_restricted(self) -> bool:
        return self.status in {
            SpamBotStatus.TEMPORARY_RESTRICTION,
            SpamBotStatus.ACTIVE_RESTRICTION,
            SpamBotStatus.PERMANENT_RESTRICTION,
            SpamBotStatus.RESTRICTION_UNCLEAR,
        }


_ARABIC_INDIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def _fold(value: str) -> str:
    """Case-fold, remove diacritics and normalize whitespace for matching."""
    decomposed = unicodedata.normalize("NFKD", value.translate(_ARABIC_INDIC_DIGITS)).casefold()
    without_marks = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return " ".join(without_marks.split())


def _markers(*values: str) -> tuple[str, ...]:
    return tuple(_fold(value) for value in values)


# The messages below cover the current common @SpamBot locales.  The parser is
# deliberately marker-based rather than depending on complete bot sentences:
# Telegram can change surrounding copy without changing the safety meaning.
_FREE_MARKERS = _markers(
    "free from any restrictions",
    "free from all restrictions",
    "free of any restrictions",
    "no limits are currently applied",
    "no limitations are currently applied",
    "no restrictions are currently applied",
    "no restrictions apply to your account",
    "no restrictions have been applied to your account",
    "your account has no restrictions",
    "your account is not limited",
    "all restrictions have been removed from your account",
    "all limitations have been lifted from your account",
    "you're free as a bird",
    "you are free as a bird",
    "свободен от каких-либо ограничений",
    "никаких ограничений",
    "не має жодних обмежень",
    "немає жодних обмежень",
    "no tiene restricciones",
    "sin restricciones",
    "sem restrições",
    "sem restricoes",
    "keinen einschränkungen",
    "keinen einschrankungen",
    "keine einschränkungen",
    "keine einschrankungen",
    "aucune restriction",
    "sans restriction",
    "nessuna limitazione",
    "nessuna restrizione",
    "brak ograniczeń",
    "brak ograniczen",
    "herhangi bir kısıtlama yok",
    "herhangi bir kisitlama yok",
    "tidak ada batasan",
    "لا توجد قيود",
    "没有任何限制",
    "没有限制",
    "制限はありません",
    "제한이 없습니다",
)

_TEMPORARY_MARKERS = _markers(
    "limited until",
    "restricted until",
    "restriction until",
    "will remain limited until",
    "will remain restricted until",
    "limitations will be lifted on",
    "restrictions will be lifted on",
    "limitations will be removed on",
    "restrictions will be removed on",
    "limited through",
    "restricted through",
    "ограничен до",
    "ограничена до",
    "ограничено до",
    "ограничения действуют до",
    "обмежено до",
    "обмежений до",
    "обмежена до",
    "обмеження до",
    "hasta",
    "limitada hasta",
    "limitado hasta",
    "até",
    "ate",
    "eingeschränkt bis",
    "eingeschrankt bis",
    "beschränkt bis",
    "beschrankt bis",
    "jusqu'au",
    "jusqu a",
    "limité jusqu",
    "limite jusqu",
    "fino al",
    "fino a",
    "ograniczone do",
    "do dnia",
    "kadar kısıtlandı",
    "kadar kisitlandi",
    "dibatasi hingga",
    "مقيد حتى",
    "مقيدة حتى",
    "限制至",
    "限制到",
    "まで制限",
    "까지 제한",
)

_RESTRICTION_MARKERS = _markers(
    "limited",
    "restricted",
    "restriction",
    "ограничен",
    "ограничена",
    "ограничени",
    "обмежен",
    "restriccion",
    "restricción",
    "limitado",
    "limitada",
    "limitado",
    "limitada",
    "eingeschränkt",
    "eingeschrankt",
    "beschränkt",
    "beschrankt",
    "limité",
    "limite",
    "limitation",
    "ograniczon",
    "kısıt",
    "kisit",
    "dibatasi",
    "مقيد",
    "قيود",
    "限制",
    "制限",
    "제한",
)

# @SpamBot can send an onboarding/question prompt before it returns a status.
# It contains words such as "limited" but is not evidence that the account is
# restricted. Keep it diagnostically unknown instead of pausing an account.
_STATUS_INQUIRY_MARKERS = _markers(
    "i can help you find out if your account was limited",
    "has your account been limited recently",
    "может помочь узнать, ограничен ли ваш аккаунт",
    "ваш аккаунт недавно был ограничен",
    "puedo ayudarte a saber si tu cuenta fue limitada",
    "posso ajudar a descobrir se sua conta foi limitada",
)

_PERMANENT_MARKERS = _markers(
    "permanent restriction",
    "permanent limitations",
    "permanently limited",
    "permanently restricted",
    "will remain limited indefinitely",
    "will remain restricted indefinitely",
    "not possible to remove the limitations",
    "cannot remove the limitations",
    "cannot be undone",
    "can not be undone",
    "serious violations",
    "навсегда",
    "бессрочно",
    "не подлежит отмене",
    "серьезные нарушения",
    "серйозні порушення",
    "назавжди",
    "permanentemente",
    "no se puede deshacer",
    "permanentemente",
    "nao pode ser desfeito",
    "nicht rückgängig gemacht werden",
    "nicht ruckgangig gemacht werden",
    "dauerhaft",
    "définitivement",
    "definitivement",
    "ne peut pas être annulée",
    "ne peut pas etre annulee",
    "permanentemente",
    "non può essere annullata",
    "non puo essere annullata",
    "na stałe",
    "na stale",
    "trwale",
    "kalıcı",
    "kalici",
    "بشكل دائم",
    "دائم",
    "永久",
    "永続",
    "영구",
)

# @SpamBot sometimes confirms a current limitation without naming its end date.
# It is neither UNKNOWN nor a permanent ban: pause and recheck it later.
_ACTIVE_RESTRICTION_MARKERS = _markers(
    "while the account is limited",
    "while your account is limited",
    "your account is currently limited",
    "your account is currently restricted",
    "you will not be able to send messages to people who do not have your number",
    "you will not be able to send messages to people who don't have your number",
    "you will not be able to send messages to people who have not contacted you first",
    "you cannot send messages to people who do not have your number",
    "you cannot send messages to people who don't have your number",
    "you can still reply to anyone who messages you first",
    "when people contact you first, you can always reply",
    "terwijl je account beperkt is",
    "пока ваш аккаунт ограничен",
    "пока аккаунт ограничен",
    "вы не сможете отправлять сообщения людям, у которых нет вашего номера",
    "вы по-прежнему можете отвечать тем, кто написал вам первым",
    "поки ваш обліковий запис обмежено",
    "ви не зможете надсилати повідомлення людям, у яких немає вашого номера",
    "ви можете відповідати тим, хто написав вам першим",
    "mientras tu cuenta esté limitada",
    "no podrás enviar mensajes a personas que no tengan tu número",
    "enquanto sua conta estiver limitada",
    "não poderá enviar mensagens para pessoas que não têm seu número",
    "solange dein konto eingeschränkt ist",
    "du kannst keine nachrichten an personen senden, die deine nummer nicht haben",
    "tant que votre compte est limité",
    "vous ne pourrez pas envoyer de messages aux personnes qui n'ont pas votre numéro",
    "finché il tuo account è limitato",
    "non potrai inviare messaggi alle persone che non hanno il tuo numero",
    "gdy twoje konto jest ograniczone",
    "nie będziesz mógł wysyłać wiadomości do osób, które nie mają twojego numeru",
    "hesabınız kısıtlandığı sürece",
    "numaranıza sahip olmayan kişilere mesaj gönderemezsiniz",
    "selama akun anda dibatasi",
    "tidak dapat mengirim pesan kepada orang yang tidak memiliki nomor anda",
    "أثناء تقييد حسابك",
    "لا يمكنك إرسال رسائل إلى الأشخاص الذين لا يملكون رقمك",
    "帐户受限期间",
    "无法向没有您电话号码的人发送消息",
    "アカウントが制限されている間",
    "電話番号を連絡先に登録していない人にメッセージを送信できません",
    "계정이 제한되는 동안",
    "전화번호를 연락처에 저장하지 않은 사람에게 메시지를 보낼 수 없습니다",
)

_ACTIVE_RESTRICTION_BUTTON_MARKERS = _markers(
    "i was wrong, please release me",
    "this is a mistake",
    "mijn fout, geef me weer vrij",
    "dit is een fout",
    "this decision is wrong",
    "i believe this is a mistake",
    "это ошибка",
    "я считаю, что это ошибка",
    "це помилка",
    "esto es un error",
    "isto é um erro",
    "das ist ein fehler",
    "c'est une erreur",
    "questo è un errore",
    "to jest błąd",
    "bu bir hata",
    "ini adalah kesalahan",
    "هذا خطأ",
    "这是一个错误",
    "これは間違いです",
    "이것은 실수입니다",
)


_DIRECT_OUTREACH_MARKERS = _markers(
    "send messages to people who do not have your number",
    "send messages to people who don't have your number",
    "send messages to people who have not contacted you first",
    "message people who are not in your contacts",
    "writing to strangers who haven't contacted you first",
    "отправлять сообщения людям, у которых нет вашего номера",
    "писать незнакомым людям, которые не написали вам первыми",
    "надсилати повідомлення людям, у яких немає вашого номера",
    "enviar mensajes a personas que no tengan tu número",
    "enviar mensagens para pessoas que não têm seu número",
    "nachrichten an personen senden, die deine nummer nicht haben",
    "envoyer des messages aux personnes qui n'ont pas votre numéro",
    "inviare messaggi alle persone che non hanno il tuo numero",
    "wysyłać wiadomości do osób, które nie mają twojego numeru",
    "numaranıza sahip olmayan kişilere mesaj gönderemezsiniz",
    "mengirim pesan kepada orang yang tidak memiliki nomor anda",
    "إرسال رسائل إلى الأشخاص الذين لا يملكون رقمك",
    "向没有您电话号码的人发送消息",
    "電話番号を連絡先に登録していない人にメッセージを送信",
    "전화번호를 연락처에 저장하지 않은 사람에게 메시지를",
)

_INVITE_RESTRICTION_MARKERS = _markers(
    "add them to groups and channels",
    "add people to groups and channels",
    "adding people to groups and channels",
    "добавлять их в группы и каналы",
    "добавлять людей в группы и каналы",
    "додавати їх до груп і каналів",
    "agregarlas a grupos y canales",
    "adicionar pessoas a grupos e canais",
    "zu gruppen und kanälen hinzufügen",
    "ajouter des personnes aux groupes et aux chaînes",
    "aggiungerle a gruppi e canali",
    "dodawać ich do grup i kanałów",
    "gruplara ve kanallara ekleyemezsiniz",
    "menambahkannya ke grup dan kanal",
    "إضافتهم إلى المجموعات والقنوات",
    "将他们添加到群组和频道",
    "グループやチャンネルに追加",
    "그룹과 채널에 추가",
)

_INBOUND_REPLY_ALLOWED_MARKERS = _markers(
    "when people contact you first, you can always reply",
    "you can always reply to them",
    "you can still reply to anyone who messages you first",
    "remember that you can still reply to anyone who messages you first",
    "можете отвечать тем, кто написал вам первым",
    "можете по-прежнему отвечать тем, кто написал вам первым",
    "можете відповідати тим, хто написав вам першим",
    "puedes responder a quienes te escriban primero",
    "pode responder a quem falar com você primeiro",
    "kannst immer antworten, wenn dich jemand zuerst kontaktiert",
    "pouvez toujours répondre aux personnes qui vous contactent d'abord",
    "puoi sempre rispondere a chi ti contatta per primo",
    "możesz odpowiadać osobom, które najpierw do ciebie napiszą",
    "size ilk mesajı atan kişilere yanıt verebilirsiniz",
    "dapat membalas orang yang menghubungi anda terlebih dahulu",
    "يمكنك الرد على من يراسلك أولاً",
    "您仍然可以回复先联系您的人",
    "先に連絡してきた人には返信できます",
    "먼저 연락한 사람에게는 답장할 수 있습니다",
)

_MONTH_NUMBERS = {
    # English
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
    # Russian and Ukrainian
    "января": 1,
    "январь": 1,
    "янв": 1,
    "февраля": 2,
    "февраль": 2,
    "фев": 2,
    "марта": 3,
    "март": 3,
    "апреля": 4,
    "апрель": 4,
    "апр": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июнь": 6,
    "июн": 6,
    "июля": 7,
    "июль": 7,
    "июл": 7,
    "августа": 8,
    "август": 8,
    "авг": 8,
    "сентября": 9,
    "сентябрь": 9,
    "сен": 9,
    "октября": 10,
    "октябрь": 10,
    "окт": 10,
    "ноября": 11,
    "ноябрь": 11,
    "ноя": 11,
    "декабря": 12,
    "декабрь": 12,
    "дек": 12,
    "січня": 1,
    "січень": 1,
    "лютого": 2,
    "лютий": 2,
    "березня": 3,
    "березень": 3,
    "квітня": 4,
    "квітень": 4,
    "травня": 5,
    "травень": 5,
    "червня": 6,
    "червень": 6,
    "липня": 7,
    "липень": 7,
    "серпня": 8,
    "серпень": 8,
    "вересня": 9,
    "вересень": 9,
    "жовтня": 10,
    "жовтень": 10,
    "листопада": 11,
    "листопад": 11,
    "грудня": 12,
    "грудень": 12,
    # Spanish and Portuguese
    "enero": 1,
    "ene": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
    # German, French and Italian
    "januar": 1,
    "februar": 2,
    "marz": 3,
    "maerz": 3,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "oktober": 10,
    "dezember": 12,
    "janvier": 1,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    "gennaio": 1,
    "febbraio": 2,
    "aprile": 4,
    "maggio": 5,
    "giugno": 6,
    "luglio": 7,
    "settembre": 9,
    "ottobre": 10,
    "dicembre": 12,
    # Polish, Turkish and Indonesian
    "stycznia": 1,
    "styczen": 1,
    "lutego": 2,
    "luty": 2,
    "marca": 3,
    "marzec": 3,
    "kwietnia": 4,
    "kwiecien": 4,
    "maja": 5,
    "czerwca": 6,
    "czerwiec": 6,
    "lipca": 7,
    "lipiec": 7,
    "sierpnia": 8,
    "sierpien": 8,
    "wrzesnia": 9,
    "wrzesien": 9,
    "pazdziernika": 10,
    "pazdziernik": 10,
    "listopada": 11,
    "grudnia": 12,
    "grudzien": 12,
    "ocak": 1,
    "subat": 2,
    "mart": 3,
    "nisan": 4,
    "mayis": 5,
    "haziran": 6,
    "temmuz": 7,
    "agustos": 8,
    "eylul": 9,
    "ekim": 10,
    "kasim": 11,
    "aralik": 12,
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "mei": 5,
    "agustus": 8,
    "desember": 12,
}

_MONTH_PATTERN = "|".join(
    re.escape(month) for month in sorted(_MONTH_NUMBERS, key=len, reverse=True)
)
_TIME_PATTERN = r"(?P<hour>[01]?\d|2[0-3])[:.](?P<minute>[0-5]\d)(?::(?P<second>[0-5]\d))?"
_TIMEZONE_PATTERN = (
    r"\s*(?P<timezone>z|(?:utc|gmt)\s*[+-]?\s*\d{0,2}(?::?\d{2})?|[+-]\d{2}:?\d{2})?"
)
_ISO_DATE = re.compile(
    rf"(?P<year>20\d{{2}})\s*[-./]\s*(?P<month>1[0-2]|0?[1-9])\s*[-./]"
    rf"\s*(?P<day>3[01]|[12]\d|0?[1-9])(?:[ t,]+{_TIME_PATTERN}{_TIMEZONE_PATTERN})?"
)
_NUMERIC_DATE = re.compile(
    rf"(?P<num1>3[01]|[12]\d|0?[1-9])\s*[-./]\s*(?P<num2>1[0-2]|0?[1-9])"
    rf"\s*[-./]\s*(?P<year>20\d{{2}})(?:[ ,t]+{_TIME_PATTERN}{_TIMEZONE_PATTERN})?"
)
_DAY_MONTH_DATE = re.compile(
    rf"(?P<day>3[01]|[12]\d|0?[1-9])\s*(?:de\s+)?(?P<month>{_MONTH_PATTERN})\.?\s*(?:de\s+)?(?P<year>20\d{{2}})(?P<tail>.{{0,48}})",
)
_MONTH_DAY_DATE = re.compile(
    rf"(?P<month>{_MONTH_PATTERN})\s+(?P<day>3[01]|[12]\d|0?[1-9])(?:st|nd|rd|th)?\s*,?\s*(?P<year>20\d{{2}})(?P<tail>.{{0,48}})",
)
_YEARLESS_DAY_MONTH = re.compile(
    rf"(?P<day>3[01]|[12]\d|0?[1-9])\s*(?:de\s+)?(?P<month>{_MONTH_PATTERN})\.?\s*(?P<tail>.{{0,32}})",
)
_CHINESE_DATE = re.compile(
    r"(?P<year>20\d{2})\s*年\s*(?P<month>\d{1,2})\s*月\s*(?P<day>\d{1,2})\s*日?(?P<tail>.{0,32})",
)
_TIME_SEARCH = re.compile(rf"{_TIME_PATTERN}{_TIMEZONE_PATTERN}")


def parse_spambot_response(
    raw_text: str | None,
    *,
    now: datetime | None = None,
    button_rows: tuple[tuple[str, ...], ...] = (),
    format_entities: tuple[tuple[str, int, int], ...] = (),
) -> SpamBotResult:
    """Classify one @SpamBot reply and return a UTC restriction expiry when present.

    A date is accepted only in a restriction context.  This prevents unrelated
    dates in bot copy from being interpreted as an account cooldown.
    """
    raw = _safe_raw_text(raw_text)
    text = _fold(raw)
    if not text:
        return SpamBotResult(
            SpamBotStatus.UNKNOWN,
            raw,
            parser_signal="empty_response",
            button_rows=button_rows,
            format_entities=format_entities,
        )

    reference = _utc_now(now)
    restriction_scope, allows_reply_to_inbound = _restriction_capabilities(text)
    restricted_until = _find_restriction_expiry(text, reference)
    if restricted_until is not None:
        return SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION,
            raw,
            restricted_until=restricted_until,
            parser_signal="restriction_marker_with_date",
            button_rows=button_rows,
            format_entities=format_entities,
            restriction_scope=restriction_scope,
            allows_reply_to_inbound=allows_reply_to_inbound,
        )
    if _contains_any(text, _PERMANENT_MARKERS):
        return SpamBotResult(
            SpamBotStatus.PERMANENT_RESTRICTION,
            raw,
            parser_signal="permanent_marker",
            button_rows=button_rows,
            format_entities=format_entities,
            restriction_scope=restriction_scope,
            allows_reply_to_inbound=allows_reply_to_inbound,
        )
    if _contains_any(text, _FREE_MARKERS):
        return SpamBotResult(
            SpamBotStatus.FREE,
            raw,
            parser_signal="free_marker",
            button_rows=button_rows,
            format_entities=format_entities,
        )
    button_text = _fold(" ".join(button for row in button_rows for button in row))
    active_restriction = _contains_any(text, _ACTIVE_RESTRICTION_MARKERS) or (
        _contains_any(text, _RESTRICTION_MARKERS)
        and _contains_any(button_text, _ACTIVE_RESTRICTION_BUTTON_MARKERS)
    )
    if active_restriction:
        return SpamBotResult(
            SpamBotStatus.ACTIVE_RESTRICTION,
            raw,
            parser_signal="active_restriction_without_expiry",
            button_rows=button_rows,
            format_entities=format_entities,
            restriction_scope=restriction_scope,
            allows_reply_to_inbound=allows_reply_to_inbound,
        )
    if _contains_any(text, _STATUS_INQUIRY_MARKERS):
        return SpamBotResult(
            SpamBotStatus.UNKNOWN,
            raw,
            parser_signal="status_inquiry_without_verdict",
            button_rows=button_rows,
            format_entities=format_entities,
        )
    if _contains_any(text, _RESTRICTION_MARKERS):
        return SpamBotResult(
            SpamBotStatus.RESTRICTION_UNCLEAR,
            raw,
            parser_signal="restriction_confirmed_scope_unclear",
            button_rows=button_rows,
            format_entities=format_entities,
            restriction_scope=SpamBotRestrictionScope.UNSPECIFIED,
            allows_reply_to_inbound=allows_reply_to_inbound,
        )
    return SpamBotResult(
        SpamBotStatus.UNKNOWN,
        raw,
        parser_signal="unrecognised_response",
        button_rows=button_rows,
        format_entities=format_entities,
    )


def _safe_raw_text(value: str | None) -> str:
    if not value:
        return ""
    safe_characters = (
        character for character in str(value) if character == "\n" or ord(character) >= 32
    )
    return "".join(safe_characters)[:4000]


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _restriction_capabilities(text: str) -> tuple[SpamBotRestrictionScope, bool | None]:
    """Extract only capabilities explicitly stated by @SpamBot.

    A restricted account might still reply to an inbound private message or
    have access to an existing chat.  Neither fact is a permission to use it
    for automated delivery, so this function never declares chat publishing
    available.  It reports the narrow, textual guarantees only.
    """

    direct_outreach_blocked = _contains_any(text, _DIRECT_OUTREACH_MARKERS)
    invites_blocked = _contains_any(text, _INVITE_RESTRICTION_MARKERS)
    allows_reply_to_inbound = True if _contains_any(text, _INBOUND_REPLY_ALLOWED_MARKERS) else None
    if direct_outreach_blocked and invites_blocked:
        return SpamBotRestrictionScope.DIRECT_OUTREACH_AND_INVITES, allows_reply_to_inbound
    if direct_outreach_blocked:
        return SpamBotRestrictionScope.DIRECT_OUTREACH, allows_reply_to_inbound
    return SpamBotRestrictionScope.UNSPECIFIED, allows_reply_to_inbound


def _find_restriction_expiry(text: str, now: datetime) -> datetime | None:
    marker_positions = [
        position for marker in _TEMPORARY_MARKERS for position in _positions(text, marker)
    ]
    if not marker_positions:
        return None
    for position in sorted(marker_positions):
        expiry = _parse_datetime(text[position : position + 180], now)
        if expiry is not None:
            return expiry
    return _parse_datetime(text, now)


def _positions(text: str, marker: str) -> tuple[int, ...]:
    positions: list[int] = []
    start = 0
    while (position := text.find(marker, start)) >= 0:
        positions.append(position)
        start = position + len(marker)
    return tuple(positions)


def _parse_datetime(text: str, now: datetime) -> datetime | None:
    for pattern in (_ISO_DATE, _NUMERIC_DATE):
        if match := pattern.search(text):
            if pattern is _NUMERIC_DATE:
                first = int(match["num1"])
                second = int(match["num2"])
                # Day-month is the safest cross-locale default.  For clearly
                # US-only values (15/08) it is also unambiguous.
                day, month = first, second
            else:
                day, month = int(match["day"]), int(match["month"])
            return _make_datetime(
                int(match["year"]),
                month,
                day,
                match.groupdict(),
            )
    for pattern in (_DAY_MONTH_DATE, _MONTH_DAY_DATE, _CHINESE_DATE):
        if match := pattern.search(text):
            month_value = match["month"]
            month = int(month_value) if month_value.isdigit() else _MONTH_NUMBERS.get(month_value)
            if month is None:
                continue
            groups = match.groupdict()
            if groups.get("tail"):
                time_match = _TIME_SEARCH.search(groups["tail"])
                if time_match:
                    groups.update(time_match.groupdict())
            return _make_datetime(
                int(match["year"]),
                month,
                int(match["day"]),
                groups,
            )
    if match := _YEARLESS_DAY_MONTH.search(text):
        month = _MONTH_NUMBERS.get(match["month"])
        if month is not None:
            groups = match.groupdict()
            if groups.get("tail"):
                time_match = _TIME_SEARCH.search(groups["tail"])
                if time_match:
                    groups.update(time_match.groupdict())
            candidate = _make_datetime(now.year, month, int(match["day"]), groups)
            if candidate is not None and candidate < now - timedelta(days=1):
                return _make_datetime(now.year + 1, month, int(match["day"]), groups)
            return candidate
    return None


def _make_datetime(
    year: int,
    month: int,
    day: int,
    groups: dict[str, str | None],
) -> datetime | None:
    try:
        hour = int(groups["hour"]) if groups.get("hour") is not None else 23
        minute = int(groups["minute"]) if groups.get("minute") is not None else 59
        second = (
            int(groups["second"])
            if groups.get("second") is not None
            else 0
            if groups.get("hour") is not None
            else 59
        )
        offset = _timezone_offset(groups.get("timezone"))
        return datetime(year, month, day, hour, minute, second, tzinfo=offset).astimezone(UTC)
    except ValueError:
        return None


def _timezone_offset(value: str | None) -> timezone:
    if not value:
        return UTC
    normalized = value.replace(" ", "").upper()
    if normalized in {"Z", "UTC", "GMT"}:
        return UTC
    for prefix in ("UTC", "GMT"):
        if normalized.startswith(prefix):
            normalized = normalized.removeprefix(prefix)
            break
    if not normalized:
        return UTC
    sign = -1 if normalized.startswith("-") else 1
    digits = normalized.lstrip("+-").replace(":", "")
    if not digits:
        return UTC
    hours = int(digits[:2])
    minutes = int(digits[2:] or 0)
    if hours > 23 or minutes > 59:
        return UTC
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _utc_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
