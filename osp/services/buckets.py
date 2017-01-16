

import os
import boto
import io
import attr

from itertools import islice
from cached_property import cached_property
from boto.s3.key import Key

from osp import settings


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
        for i, key in enumerate(self.bucket.list(crawl+'/')):

            yield key.name

            # TODO|dev
            if i % 1000 == 0:
                print(i)

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
        path = os.path.join('text4', '{}.txt'.format(record_id))

        # Write text.
        key = Key(self.bucket)
        key.key = path
        key.set_contents_from_string(text)
