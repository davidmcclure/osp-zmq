

import os
import io
import boto

from boto.s3.key import Key

from osp.services import config
from osp.scraper_warc import ScraperWARC


class ListWARCPaths:

    @classmethod
    def from_env(cls):
        return cls(config['buckets']['scraper'])

    def __init__(self, bucket):
        """Create the bucket instance.

        Args:
            bucket (str): Name of the scraper bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(bucket)

    def __call__(self):
        """Generate WARC paths.

        Returns: iter
        """
        for key in self.bucket.list():
            yield dict(warc_path=key.name)


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
