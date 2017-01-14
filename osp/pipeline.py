

import boto
import os

from itertools import islice
from boto.s3.key import Key
from cached_property import cached_property

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL

from osp.services import config
from osp.scraper_warc import ScraperWARC
from osp import settings


class Singleton:

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def reset(self):
        del self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')


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


@Singleton
class ScraperBucket:

    def __init__(self):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(settings.SCRAPER_BUCKET)

    def paths(self, crawl):
        """Get all WARC paths in a crawl directory.
        """
        for key in self.bucket.list(crawl+'/'):
            yield key.name

    def first_n_paths(self, crawl, n):
        """Skim off the first N paths in a crawl directory.
        """
        yield from islice(self.paths(crawl), n)


@Singleton
class ResultBucket:

    def __init__(self):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(settings.RESULT_BUCKET)

    def write_text(self, record_id, text):
        """Write extracted text for a document.
        """
        # Form S3 path.
        path = os.path.join('text', '{}.txt'.format(record_id))

        # Write text.
        key = Key(self.bucket)
        key.key = path
        key.set_contents_from_string(text)


class ExtractText:

    # TODO: Where to parametrize?

    def __init__(self):
        """Set input buckets.

        Args:
            warc_bucket (str): Scraper bucket.
            text_bucket (str): Result bucket.
        """
        self.warc_bucket = ScraperBucket.from_env()
        self.text_bucket = ResultBucket.from_env()

    def __call__(self, path):
        """Extract text, write to S3.

        Args:
            warc_path (str): WARC S3 path.
        """
        warc = ScraperWARC.from_s3(path)

        text = warc.text()

        record_id = warc.record_id()

        if text:
            self.text_bucket.write_text(record_id, text)

        return dict(
            corpus='syllascrape',
            identifier=record_id,
            url=warc.url(),
        )
