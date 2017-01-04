

import os
import anyconfig

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL


class Config(dict):

    @classmethod
    def from_env(cls):
        """Get a config instance with the default files.
        """
        root = os.environ.get('OSP_CONFIG', '/etc/osp')

        # Default paths.
        paths = [
            os.path.join(os.path.dirname(__file__), 'config.yml'),
            os.path.join(root, 'osp.yml'),
        ]

        return cls(paths)

    def __init__(self, paths):
        """Initialize the configuration object.

        Args:
            paths (list): YAML paths, from most to least specific.
        """
        config = anyconfig.load(paths, ignore_missing=True)

        return super().__init__(config)

    def build_sqla_url(self):
        """Build an SQLAlchemy connection string.

        Returns: URL
        """
        return URL(**dict(
            drivername='sqlite',
            database=self['database'],
        ))

    def build_sqla_engine(self):
        """Build a SQLAlchemy engine.

        Returns: Engine
        """
        url = self.build_sqla_url()

        engine = create_engine(url)

        # Fix transaction bugs in pysqlite.

        @event.listens_for(engine, 'connect')
        def connect(conn, record):
            conn.isolation_level = None

        @event.listens_for(engine, 'begin')
        def begin(conn):
            conn.execute('BEGIN')

        return engine

    def build_sqla_session(self):
        """Build a scoped session manager.

        Returns: Session
        """
        engine = self.build_sqla_engine()

        factory = sessionmaker(bind=engine)

        return scoped_session(factory)
