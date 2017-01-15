

import os
import boto
import io

from itertools import islice
from boto.s3.key import Key
from cached_property import cached_property

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL

from osp import settings


class Singleton:

    def __init__(self, decorated):
        self._decorated = decorated

    def __call__(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def reset(self):
        del self._instance


@Singleton
class Database:

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


class Bucket:

    def __init__(self, name):
        """Connect to the bucket.

        Args:
            name (str): Bucket name.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(name)

    def read_bytes(self, path):
        """Read an object into BytesIO.

        Args:
            path (str): Key path.

        Returns: io.BytesIO
        """
        key = self.bucket.get_key(path)
        return io.BytesIO(key.get_contents_as_string())


@Singleton
class ScraperBucket(Bucket):

    def __init__(self):
        super().__init__(settings.SCRAPER_BUCKET)

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


@Singleton
class ResultBucket(Bucket):

    def __init__(self):
        super().__init__(settings.RESULT_BUCKET)

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
