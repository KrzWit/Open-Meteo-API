"""Database setup and session management."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.models import Base


def create_db_engine(settings: Settings):
    """Create SQLAlchemy engine for PostgreSQL."""

    return create_engine(settings.database_url, future=True)


def create_session_factory(engine):
    """Create a configured SQLAlchemy session factory."""

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def initialize_database(engine) -> None:
    """Create project tables if they do not already exist."""

    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session(session_factory) -> Generator[Session, None, None]:
    """Provide a transactional session scope."""

    session: Session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
