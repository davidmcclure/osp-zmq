

import os
import anyconfig
import boto

from cached_property import cached_property

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL


class Config:

    _config = {}
    _cache = {}

    @classmethod
    def read(cls):
        """Read configuration from files.
        """

        root = os.environ.get('OSP_CONFIG', '/etc/osp')

        # Default paths.
        paths = [
            os.path.join(os.path.dirname(__file__), 'config.yml'),
            os.path.join(root, 'osp.yml'),
        ]

        cls._config = anyconfig.load(paths, ignore_missing=True)

    def __init__(self):
        """Hyrdate configuration.
        """
        if not self._config:
            self.read()

        self.__dict__ = self._cache

    def __getitem__(self, key):
        return self._config[key]


class Database(Config):

    @cached_property
    def url(self):
        """Build an SQLAlchemy connection string.

        Returns: URL
        """
        return URL(**dict(
            drivername='sqlite',
            database=self['database'],
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


class Buckets(Config):

    @cached_property
    def s3(self):
        return boto.connect_s3()

    @cached_property
    def scraper(self):
        return self.s3.get_bucket(self['buckets']['scraper'])
