

import os
import io
import boto

from boto.s3.key import Key

from osp.services import config
from osp.scraper_warc import ScraperWARC


class SyllascrapeBucket:

    def __init__(self, name):
        """Connect to the bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(name)

    def site_directories(self, crawl):
        """Get the full list of run directories for a crawl.

        Args:
            crawl (str): The crawl identifier.

        Returns: A list of uuids.
        """
        for key in self.bucket.list(crawl+'/', '/'):
            yield os.path.basename(key.name.strip('/'))



class ListWARCPaths:

    @classmethod
    def from_env(cls):
        return cls(config['buckets']['scraper'])

    def __init__(self, bucket, spider_run='oct-16', prefixes=None):
        """Create the bucket instance.

        Args:
            bucket (str): Name of the scraper bucket.
            spider_run (str): The spider run directory.
            prefixes (list): Only include crawl directories with uuids that
                start with a value in this list.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(bucket)

        self.spider_run = spider_run
        self.prefixes = prefixes

    def __call__(self):
        """Generate WARC paths.

        Returns: iter
        """
        dirs = self.bucket.list(self.spider_run+'/', '/')

        for d in dirs:
            yield d.name

        # for key in self.bucket.list():
            # yield key.name


class ParseWARC:

    @classmethod
    def from_env(cls):
        return cls(
            config['buckets']['scraper'],
            config['buckets']['results'],
            config['dirs']['text'],
        )

    def __init__(self, warc_bucket, text_bucket, text_dir):
        """Set input + output buckets.

        Args:
            warc_bucket (str): Scraper bucket.
            text_bucket (str): Text bucket.
            text_dir (str): Path prefix for extracted text.
        """
        s3 = boto.connect_s3()

        self.warc_bucket = s3.get_bucket(warc_bucket)
        self.text_bucket = s3.get_bucket(text_bucket)

        self.text_dir = text_dir

    def __call__(self, warc_path):
        """Extract text, write to S3.

        Args:
            warc_path (str): WARC S3 path.
        """
        # Pull WARC from S3.
        key = self.warc_bucket.get_key(warc_path)
        blob = io.BytesIO(key.get_contents_as_string())

        # Extract text.
        warc = ScraperWARC(blob)
        text = warc.text()

        record_id = warc.record_id()

        if text:

            # Form S3 path.
            text_path = os.path.join(
                self.text_dir,
                '{}.txt'.format(record_id),
            )

            # Write text.
            text_key = Key(self.text_bucket)
            text_key.key = text_path
            text_key.set_contents_from_string(text)

        return dict(
            corpus='syllascrape',
            identifier=record_id,
            url=warc.url(),
        )
