from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from proxima_traffic.config import get_settings
from proxima_traffic.models import Base

SQLITE_PREFIX = "sqlite+aiosqlite:///"
REQUIRED_SEEDED_ROW_COUNTS = {"mention_blacklist_entries": 1057}
REQUIRED_TABLES = frozenset((*Base.metadata.tables, "alembic_version"))
REQUIRED_COLUMNS = {
    table_name: frozenset(column.name for column in table.columns)
    for table_name, table in Base.metadata.tables.items()
}


def _missing_seed_data(
    connection: sqlite3.Connection,
    tables: set[str],
) -> list[str]:
    missing: list[str] = []
    for table_name, minimum_row_count in REQUIRED_SEEDED_ROW_COUNTS.items():
        count = 0
        if table_name in tables:
            row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            count = int(row[0]) if row else 0
        if count < minimum_row_count:
            missing.append(
                f"{table_name}: expected at least {minimum_row_count} rows, found {count}"
            )
    return missing


@dataclass(frozen=True, slots=True)
class DoctorReport:
    database_path: str
    exists: bool
    integrity: str
    foreign_key_violations: int
    migration_revision: str | None
    expected_migration_revision: str
    migration_current: bool
    missing_tables: list[str]
    missing_columns: list[str]
    missing_seed_data: list[str]

    @property
    def healthy(self) -> bool:
        return (
            self.exists
            and self.integrity == "ok"
            and self.foreign_key_violations == 0
            and self.migration_current
            and not self.missing_tables
            and not self.missing_columns
            and not self.missing_seed_data
        )


@dataclass(frozen=True, slots=True)
class MediaGCReport:
    storage_path: str
    scanned_files: int
    referenced_files: int
    skipped_recent_files: int
    candidate_files: list[str]
    removed_files: list[str]


@dataclass(frozen=True, slots=True)
class CleanDatabaseReport:
    source_path: str
    destination_path: str
    migration_revision: str
    copied_rows: dict[str, int]


@dataclass(frozen=True, slots=True)
class BackupVerification:
    path: str
    size_bytes: int
    sha256: str
    integrity: str
    foreign_key_violations: int


def sqlite_path_from_url(database_url: str) -> Path:
    if not database_url.startswith(SQLITE_PREFIX):
        raise ValueError("maintenance commands currently support only sqlite+aiosqlite URLs")
    raw_path = database_url.removeprefix(SQLITE_PREFIX)
    if not raw_path:
        raise ValueError("SQLite database path is empty")
    return Path(raw_path).expanduser().resolve()


@lru_cache(maxsize=1)
def expected_migration_revision() -> str:
    """Resolve the single Alembic head bundled with this application checkout."""
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    heads = ScriptDirectory.from_config(config).get_heads()
    if len(heads) != 1:
        raise RuntimeError(f"expected exactly one Alembic head, found {len(heads)}")
    return heads[0]


def create_sqlite_backup(database_url: str, destination_directory: Path) -> Path:
    source_path = sqlite_path_from_url(database_url)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    destination_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = destination_directory / f"proxima-{timestamp}.db"
    suffix = 1
    while destination.exists():
        destination = destination_directory / f"proxima-{timestamp}-{suffix}.db"
        suffix += 1
    with sqlite3.connect(source_path) as source, sqlite3.connect(destination) as target:
        source.backup(target)
    verification = verify_sqlite_backup(destination)
    if verification.integrity != "ok" or verification.foreign_key_violations:
        destination.unlink(missing_ok=True)
        raise RuntimeError("created backup failed SQLite integrity verification")
    return destination


def verify_sqlite_backup(path: Path) -> BackupVerification:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    digest = hashlib.sha256()
    with resolved.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    with sqlite3.connect(resolved) as connection:
        row = connection.execute("PRAGMA quick_check").fetchone()
        integrity = str(row[0]) if row else "unknown"
        foreign_key_violations = len(connection.execute("PRAGMA foreign_key_check").fetchall())
    return BackupVerification(
        path=str(resolved),
        size_bytes=resolved.stat().st_size,
        sha256=digest.hexdigest(),
        integrity=integrity,
        foreign_key_violations=foreign_key_violations,
    )


def create_clean_database(
    source_database_url: str,
    destination_path: Path,
) -> CleanDatabaseReport:
    """Create an empty operational database while retaining selected configuration."""
    source_path = sqlite_path_from_url(source_database_url)
    destination = destination_path.expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    source_report = inspect_sqlite(source_database_url)
    if not source_report.healthy:
        raise ValueError("source database must pass doctor before creating a clean database")
    if destination == source_path:
        raise ValueError("destination must differ from the source database")
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    destination_url = f"sqlite+aiosqlite:///{destination.as_posix()}"
    config.set_main_option("sqlalchemy.url", destination_url.replace("%", "%%"))
    config.attributes["database_url"] = destination_url
    migration_error: list[BaseException] = []

    def upgrade_clean_database() -> None:
        try:
            # Alembic's async env owns an event loop.  This maintenance API is
            # intentionally synchronous and can be called from the control
            # bot's running loop, so execute the upgrade in its own thread.
            command.upgrade(config, "head")
        except BaseException as exc:  # noqa: BLE001 - re-raise in the caller thread
            migration_error.append(exc)

    migration_thread = threading.Thread(target=upgrade_clean_database, daemon=True)
    migration_thread.start()
    migration_thread.join()
    if migration_error:
        raise migration_error[0]

    copied_rows: dict[str, int] = {}
    with sqlite3.connect(source_path) as source, sqlite3.connect(destination) as target:
        source.row_factory = sqlite3.Row
        revision_row = source.execute("SELECT version_num FROM alembic_version").fetchone()
        if revision_row is None or not isinstance(revision_row[0], str):
            raise ValueError("source database has no Alembic revision")
        for table_name in ("accounts", "proxy_groups", "proxy_endpoints", "proxy_assignments"):
            copied_rows[table_name] = _copy_sqlite_table(source, target, table_name)
        copied_rows["targets"] = _copy_targets_as_standalone_chats(source, target)
        target.commit()
    return CleanDatabaseReport(
        str(source_path),
        str(destination),
        revision_row[0],
        copied_rows,
    )


def _copy_sqlite_table(source: sqlite3.Connection, target: sqlite3.Connection, table: str) -> int:
    columns = [str(row[1]) for row in source.execute(f'PRAGMA table_info("{table}")')]
    if not columns:
        return 0
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    rows = source.execute(f'SELECT {quoted_columns} FROM "{table}"').fetchall()
    target.executemany(
        f'INSERT INTO "{table}" ({quoted_columns}) VALUES ({placeholders})',
        [tuple(row[column] for column in columns) for row in rows],
    )
    return len(rows)


def _copy_targets_as_standalone_chats(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
) -> int:
    columns = [str(row[1]) for row in source.execute('PRAGMA table_info("targets")')]
    if not columns:
        return 0
    list_approvals = {
        int(row[0]): bool(row[1])
        for row in source.execute(
            """
            SELECT target_id, MAX(enabled)
            FROM target_list_items
            GROUP BY target_id
            """
        )
    }
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    copied: list[tuple[object, ...]] = []
    for row in source.execute(f'SELECT {quoted_columns} FROM "targets"'):
        values = {column: row[column] for column in columns}
        values["is_standalone"] = True
        values["approved_by_operator"] = bool(
            values.get("approved_by_operator") or list_approvals.get(int(values["id"]), False)
        )
        copied.append(tuple(values[column] for column in columns))
    target.executemany(
        f'INSERT INTO "targets" ({quoted_columns}) VALUES ({placeholders})',
        copied,
    )
    return len(copied)


def cleanup_media_orphans(
    database_url: str,
    storage_path: Path,
    *,
    min_age_hours: int = 24,
    apply: bool = False,
) -> MediaGCReport:
    """Find or explicitly remove unreferenced files below the configured media root."""
    if min_age_hours < 1:
        raise ValueError("min_age_hours must be at least 1")
    root = storage_path.expanduser().resolve()
    if not root.exists():
        return MediaGCReport(str(root), 0, 0, 0, [], [])
    if not root.is_dir():
        raise ValueError(f"media storage path is not a directory: {root}")

    references = _media_references(database_url, root)
    cutoff = datetime.now(UTC) - timedelta(hours=min_age_hours)
    scanned = 0
    skipped_recent = 0
    candidates: list[str] = []
    removed: list[str] = []
    for candidate in root.rglob("*"):
        if not candidate.is_file() or candidate.is_symlink():
            continue
        scanned += 1
        resolved = candidate.resolve()
        if not resolved.is_relative_to(root):
            continue
        if resolved in references:
            continue
        modified_at = datetime.fromtimestamp(candidate.stat().st_mtime, UTC)
        if modified_at > cutoff:
            skipped_recent += 1
            continue
        candidates.append(str(resolved))
        if apply:
            candidate.unlink()
            removed.append(str(resolved))
    return MediaGCReport(
        str(root),
        scanned,
        len(references),
        skipped_recent,
        candidates,
        removed,
    )


def _media_references(database_url: str, storage_root: Path) -> frozenset[Path]:
    database_path = sqlite_path_from_url(database_url)
    if not database_path.is_file():
        raise FileNotFoundError(database_path)
    raw_paths: set[str] = set()
    with sqlite3.connect(database_path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if "campaigns" in tables:
            for media_path, variants_json, dialogue_json in connection.execute(
                "SELECT primary_media_path, content_variants_json, dialogue_steps_json "
                "FROM campaigns"
            ):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
                raw_paths.update(_media_paths_from_json(variants_json))
                raw_paths.update(_media_paths_from_json(dialogue_json))
        if "campaign_runs" in tables:
            for media_path, media_json, variants_json, dialogue_json in connection.execute(
                """
                SELECT primary_media_path_snapshot, media_items_snapshot_json,
                       content_variants_snapshot_json, dialogue_steps_snapshot_json
                FROM campaign_runs
                """
            ):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
                raw_paths.update(_media_paths_from_json(media_json))
                raw_paths.update(_media_paths_from_json(variants_json))
                raw_paths.update(_media_paths_from_json(dialogue_json))
        if "direct_campaigns" in tables:
            for (media_path,) in connection.execute(
                "SELECT primary_media_path FROM direct_campaigns"
            ):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
        if "direct_launches" in tables:
            for (media_path,) in connection.execute(
                "SELECT primary_media_path_snapshot FROM direct_launches"
            ):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
        if "story_tagging_campaigns" in tables:
            for (media_path,) in connection.execute("SELECT media_path FROM story_tagging_campaigns"):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
        if "channel_comment_rules" in tables:
            for media_path, dialogue_steps_json in connection.execute(
                "SELECT primary_media_path, dialogue_steps_json FROM channel_comment_rules"
            ):
                if isinstance(media_path, str):
                    raw_paths.add(media_path)
                raw_paths.update(_media_paths_from_json(dialogue_steps_json))
        if "bot_fsm_states" in tables:
            for (draft_json,) in connection.execute("SELECT data_json FROM bot_fsm_states"):
                raw_paths.update(_media_paths_from_json(draft_json))
    return frozenset(
        resolved
        for raw_path in raw_paths
        if (resolved := _managed_media_path(raw_path, storage_root)) is not None
    )


def _media_paths_from_json(raw_json: object) -> set[str]:
    if not isinstance(raw_json, str):
        return set()
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return set()
    paths: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, dict):
            for key, path in value.items():
                if (
                    key in {"path", "media_path", "profile_avatar_draft_path"}
                    or key.endswith("_media_path")
                ) and isinstance(path, str):
                    paths.add(path)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(payload)
    return paths


def _managed_media_path(raw_path: str, storage_root: Path) -> Path | None:
    candidate = Path(raw_path).expanduser()
    resolved = candidate.resolve()
    try:
        resolved.relative_to(storage_root)
    except ValueError:
        return None
    return resolved


def inspect_sqlite(database_url: str) -> DoctorReport:
    path = sqlite_path_from_url(database_url)
    expected_revision = expected_migration_revision()
    if not path.is_file():
        return DoctorReport(
            str(path),
            False,
            "missing",
            0,
            None,
            expected_revision,
            False,
            sorted(REQUIRED_TABLES),
            [],
            sorted(REQUIRED_SEEDED_ROW_COUNTS),
        )
    with sqlite3.connect(path) as connection:
        integrity_row = connection.execute("PRAGMA integrity_check").fetchone()
        integrity = str(integrity_row[0]) if integrity_row else "unknown"
        foreign_key_violations = len(connection.execute("PRAGMA foreign_key_check").fetchall())
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        revision = None
        if "alembic_version" in tables:
            row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
            revision = str(row[0]) if row else None
        missing_columns: list[str] = []
        for table, required in REQUIRED_COLUMNS.items():
            if table not in tables:
                continue
            present = {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}
            missing_columns.extend(f"{table}.{column}" for column in sorted(required - present))
        missing_seed_data = _missing_seed_data(connection, tables)
    return DoctorReport(
        database_path=str(path),
        exists=True,
        integrity=integrity,
        foreign_key_violations=foreign_key_violations,
        migration_revision=revision,
        expected_migration_revision=expected_revision,
        migration_current=revision == expected_revision,
        missing_tables=sorted(REQUIRED_TABLES - tables),
        missing_columns=sorted(missing_columns),
        missing_seed_data=missing_seed_data,
    )


def backup_command() -> None:
    parser = argparse.ArgumentParser(description="Create a consistent SQLite backup.")
    settings = get_settings()
    parser.add_argument(
        "--destination",
        default=settings.backup_storage_path,
        help="Backup directory",
    )
    arguments = parser.parse_args()
    destination = create_sqlite_backup(
        settings.database_url,
        Path(arguments.destination),
    )
    print(destination)


def doctor_command() -> None:
    report = inspect_sqlite(get_settings().database_url)
    output = {**asdict(report), "healthy": report.healthy}
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if not report.healthy:
        raise SystemExit(1)


def media_gc_command() -> None:
    parser = argparse.ArgumentParser(description="List or safely remove orphaned media files.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete eligible orphan files",
    )
    parser.add_argument(
        "--min-age-hours",
        type=int,
        default=24,
        help="Only consider files at least this old (default: 24)",
    )
    arguments = parser.parse_args()
    settings = get_settings()
    report = cleanup_media_orphans(
        settings.database_url,
        Path(settings.media_storage_path),
        min_age_hours=arguments.min_age_hours,
        apply=arguments.apply,
    )
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))


def clean_database_command() -> None:
    parser = argparse.ArgumentParser(
        description="Create a clean DB with accounts, proxies and chats only."
    )
    parser.add_argument("--destination", required=True, help="Path for the new SQLite database")
    arguments = parser.parse_args()
    report = create_clean_database(
        get_settings().database_url,
        Path(arguments.destination),
    )
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
