"""
=============================================================================
Uber Lakehouse Platform - Database Connection & Session Management
=============================================================================
Description: Manages SQLAlchemy database engine creation, connection pooling,
             and thread-safe context managers for transactional operations.
=============================================================================
"""

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from generator.config import DATABASE_URL

# Intuition: Pre-allocate a pool of 10 connections to efficiently handle
# concurrent simulation events without establishing new TCP handshakes.
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

# Thread-safe session factory for creating isolated database transactions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base class for ORM models
Base = declarative_base()


@contextmanager
def get_db_session():
    """
    Context manager to handle database session lifecycles cleanly.
    Ensures transactions are committed on success, rolled back on exceptions,
    and sessions are always returned to the connection pool.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
