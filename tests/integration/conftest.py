"""Throwaway containers for integration tests, with the same images as the Compose stack."""

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from testcontainers.community.kafka import RedpandaContainer
from testcontainers.community.postgres import PostgresContainer

POSTGRES_IMAGE = "postgis/postgis:16-3.5"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.2.3"
ALEMBIC_INI = Path(__file__).parents[2] / "migrations" / "alembic.ini"


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresContainer]:
    container = PostgresContainer(POSTGRES_IMAGE, driver="psycopg")
    container.start()
    yield container
    container.stop()


@pytest.fixture
def database_url(postgres: PostgresContainer) -> Iterator[str]:
    """A fresh, empty database per test, dropped afterwards."""
    admin = create_engine(postgres.get_connection_url(), isolation_level="AUTOCOMMIT")
    name = f"test_{uuid.uuid4().hex[:12]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    yield postgres.get_connection_url().rsplit("/", 1)[0] + f"/{name}"
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
    admin.dispose()


def alembic_config(url: str) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", url)
    return config


@pytest.fixture
def alembic_cfg(database_url: str) -> Config:
    return alembic_config(database_url)


@pytest.fixture
def migrated(database_url: str, alembic_cfg: Config) -> Iterator[Engine]:
    config = alembic_cfg
    command.upgrade(config, "head")
    engine = create_engine(database_url)
    yield engine
    engine.dispose()
    # Downgrade drops the cluster-wide app role, so the next test starts clean.
    command.downgrade(config, "base")


@pytest.fixture(scope="session")
def redpanda() -> Iterator[RedpandaContainer]:
    container = RedpandaContainer(REDPANDA_IMAGE)
    container.start(timeout=120)
    yield container
    container.stop()
