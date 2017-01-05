

import os
import io
import boto

from boto.s3.key import Key
from itertools import islice

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
        # Pull WARC from S3.
        key = self.bucket.get_key(path)
        blob = io.BytesIO(key.get_contents_as_string())

        # Extract text.
        warc = ScraperWARC(blob)
        return warc.text()
