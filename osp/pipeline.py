

import os
import io
import boto
import csv
import sys
import random

from boto.s3.key import Key
from itertools import islice

from textblob.classifiers import NaiveBayesClassifier

from osp.services import config
from osp.scraper_warc import ScraperWARC
from osp.utils import read_csv


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


class ClassifySyllabus:

    # TODO|dev: Train elsewhere, load model.

    def __init__(self):
        """Train the classifier.
        """
        csv.field_size_limit(sys.maxsize)

        rows = read_csv('osp', 'data/osp-tags.csv')

        tags = [
            (row['text'], 'Syllabus' in row['tags'].split(','))
            for row in rows
            if len(row['text']) < 10000
        ]

        random.shuffle(tags)

        train = tags[:50]

        self.classifier = NaiveBayesClassifier(train)

    def __call__(self, text):
        """Return probability doc is a syllabus..

        Args:
            text (str)

        Returns:
            float: 0-1
        """
        pd = self.classifier.prob_classify(text)

        return pd.prob(True)
