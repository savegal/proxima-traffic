"""Locate Alembic resources in both a source checkout and an installed wheel."""

from pathlib import Path

from alembic.config import Config


def migration_config() -> Config:
    package = Path(__file__).resolve().parent
    source = package.parents[1]
    if (source / "alembic.ini").is_file() and (source / "migrations" / "env.py").is_file():
        ini, scripts = source / "alembic.ini", source / "migrations"
    else:
        ini, scripts = package / "_alembic.ini", package / "_migrations"
    if not ini.is_file() or not (scripts / "env.py").is_file():
        raise RuntimeError("Alembic resources are missing; reinstall the complete Proxima package")
    config = Config(str(ini))
    config.set_main_option("script_location", str(scripts).replace("%", "%%"))
    return config
