"""Alembic environment.

URL precedence: an explicit `sqlalchemy.url` option (tests set this), then
WATCHTOWER_DATABASE_URL, then the core Compose stack's Postgres.
"""

import os

from alembic import context
from sqlalchemy import create_engine

COMPOSE_URL = "postgresql+psycopg://watchtower@127.0.0.1:15432/watchtower"


def database_url() -> str:
    explicit = context.config.get_main_option("sqlalchemy.url")
    if explicit:
        return explicit
    url = os.environ.get("WATCHTOWER_DATABASE_URL", COMPOSE_URL)
    # libpq reads PGPASSWORD, so the password never needs to sit in a URL or file.
    os.environ.setdefault("PGPASSWORD", os.environ.get("POSTGRES_PASSWORD", "watchtower"))
    return url


def run() -> None:
    url = database_url()
    if context.is_offline_mode():
        context.configure(url=url, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
        return

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            context.configure(connection=connection, transaction_per_migration=True)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


run()
