

import attr

from cached_property import cached_property

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL

from osp import settings


@attr.s
class Database:

    db_path = attr.ib()

    @classmethod
    def from_settings(cls):
        return cls(db_path=settings.DATABASE)

    @cached_property
    def url(self):
        """Build an SQLAlchemy connection string.

        Returns: URL
        """
        return URL(**dict(
            drivername='sqlite',
            database=self.db_path,
        ))

    @cached_property
    def engine(self):
        """Build a SQLAlchemy engine.

        Returns: Engine
        """
        engine = create_engine(self.url)

        # Fix transaction bugs in pysqlite.

        @event.listens_for(engine, 'connect')
        def connect(conn, record):
            conn.isolation_level = None

        @event.listens_for(engine, 'begin')
        def begin(conn):
            conn.execute('BEGIN')

        return engine

    @cached_property
    def session(self):
        """Build a scoped session manager.

        Returns: Session
        """
        factory = sessionmaker(bind=self.engine)

        return scoped_session(factory)
