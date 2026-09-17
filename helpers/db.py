"""Database session helper for direct SQL Server access."""

import os
from contextlib import contextmanager
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# These are module-level variables that are created once and reused.
# Starting them as None means the engine is not built until the first
# time get_db() is actually called (lazy initialisation).
_engine = None
_SessionLocal = None


def _get_session_factory():
    global _engine, _SessionLocal

    if _SessionLocal is None:
        # Read the pyodbc connection string from the environment.
        # Example value: "Driver={ODBC Driver 17 for SQL Server};Server=...;Database=...;"
        conn_str = os.getenv("DBCONNECTIONSTRINGBEFORDRING", "")

        # pyodbc connection strings can contain special characters like { } ; =
        # that would break a URL. quote_plus percent-encodes them so SQLAlchemy
        # can safely embed the string inside its mssql+pyodbc:// URL.
        odbc_conn = quote_plus(conn_str)

        # The engine manages the connection pool to SQL Server.
        # pool_pre_ping=True tests each connection before use so stale/dead
        # connections from the pool are detected and replaced automatically.
        _engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={odbc_conn}",
            pool_pre_ping=True,
        )

        # A session factory is a callable that creates new Session objects.
        # It is not a session itself — calling _SessionLocal() produces one.
        # autoflush=False and autocommit=False mean we control when SQL is
        # sent to the database and when changes are committed.
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

    return _SessionLocal


@contextmanager
def get_db():
    """Yield a SQLAlchemy database session, then close it when done.

    A context manager is a Python construct that runs setup code before a
    `with` block and cleanup code after it — even if an exception is raised.
    The @contextmanager decorator lets us write that pattern as a generator:
    everything before `yield` is setup, the yielded value is what the caller
    receives as `db`, and everything after `yield` (here inside `finally`) is
    the cleanup.

    Usage:
        with get_db() as db:
            db.execute(...)
            db.commit()
        # db is closed automatically here

    Closing the session returns the underlying connection back to the pool.
    """

    session_factory = _get_session_factory()

    db = session_factory()

    try:
        yield db  # hand the session to the caller

    finally:
        # Always runs — whether the code inside `with` succeeded or raised.
        db.close()
