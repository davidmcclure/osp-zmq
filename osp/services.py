

import os
import boto
import io
import attr

from itertools import islice
from cached_property import cached_property
from boto.s3.key import Key

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
            database=settings.DATABASE,
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


@attr.s
class Bucket:

    name = attr.ib()

    @cached_property
    def bucket(self):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        return s3.get_bucket(self.name)

    def read_bytes(self, path):
        """Read an object into BytesIO.

        Args:
            path (str): Key path.

        Returns: io.BytesIO
        """
        key = self.bucket.get_key(path)
        return io.BytesIO(key.get_contents_as_string())


class ScraperBucket(Bucket):

    @classmethod
    def from_settings(cls):
        return cls(name=settings.SCRAPER_BUCKET)

    def paths(self, crawl):
        """Get all WARC paths in a crawl directory.

        Args:
            crawl (str): Crawl directory name.

        Yields: str
        """
        for key in self.bucket.list(crawl+'/'):
            yield key.name

    def first_n_paths(self, crawl, n):
        """Skim off the first N paths in a crawl directory.

        Args:
            crawl (str): Crawl directory name.
            n (int): Yield N paths.

        Yields: str
        """
        yield from islice(self.paths(crawl), n)


class ResultBucket(Bucket):

    @classmethod
    def from_settings(cls):
        return cls(name=settings.RESULT_BUCKET)

    def write_text(self, record_id, text):
        """Write extracted text for a document.

        Args:
            record_id (str): Record identifier.
            text (str): Extracted text.
        """
        # Form S3 path.
        path = os.path.join('text', '{}.txt'.format(record_id))

        # Write text.
        key = Key(self.bucket)
        key.key = path
        key.set_contents_from_string(text)
