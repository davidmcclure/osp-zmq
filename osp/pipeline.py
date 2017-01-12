

import boto
import os

from itertools import islice
from boto.s3.key import Key

from osp.services import config
from osp.scraper_warc import ScraperWARC


class ScraperBucket:

    @classmethod
    def from_env(cls):
        return cls(config['buckets']['scraper'])

    def __init__(self, name):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(name)

    def paths(self, crawl):
        """Get all WARC paths in a crawl directory.
        """
        for key in self.bucket.list(crawl+'/'):
            yield key.name

    def first_n_paths(self, crawl, n):
        """Skim off the first N paths in a crawl directory.
        """
        yield from islice(self.paths(crawl), n)


class ResultBucket:

    @classmethod
    def from_env(cls):
        return cls(config['buckets']['results'])

    def __init__(self, name):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(name)

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

    @classmethod
    def from_env(cls):
        return cls(config['buckets']['scraper'])

    def __init__(self, bucket):
        """Set input buckets.

        Args:
            bucket (str): Scraper bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(bucket)

    def __call__(self, path):
        """Extract text, write to S3.

        Args:
            warc_path (str): WARC S3 path.
        """
        warc = ScraperWARC.from_s3(path)
        return warc.text()
