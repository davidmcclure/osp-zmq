

import zmq
import boto
import io
import os

from boto.s3.key import Key

from osp.scraper_warc import ScraperWARC


class Ventilator:

    def __init__(self, port):
        """Initialize the push socket.
        """
        context = zmq.Context()

        self.sender = context.socket(zmq.PUSH)
        self.sender.bind('tcp://*:{}'.format(port))

    def __call__(self, tasks):
        """Broadcast tasks.

        Args:
            tasks (func): A function that generates tasks.
        """
        for task in tasks():
            self.sender.send_string(task)


class Worker:

    def __init__(self, host, port):
        """Initialize the pull socket.
        """
        context = zmq.Context()

        self.receiver = context.socket(zmq.PULL)
        self.receiver.connect('tcp://{}:{}'.format(host, port))

    def __call__(self, work):
        """Pull tasks from ventilator.

        Args:
            work (func): A worker function.
        """
        while True:

            try:
                task = self.receiver.recv_string()
                work(task)

            except Exception as e:
                print(e)


class ListWARCPaths:

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
            yield key.name


class ParseWARC:

    def __init__(self, warc_bucket, text_bucket, text_prefix):
        """Set input + output buckets.

        Args:
            warc_bucket (str): Scraper bucket.
            text_bucket (str): Text bucket.
            text_prefix (str): Path prefix inside of text bucket.
        """
        s3 = boto.connect_s3()

        self.warc_bucket = s3.get_bucket(warc_bucket)
        self.text_bucket = s3.get_bucket(text_bucket)

        self.text_prefix = text_prefix

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

        if text:

            # Form S3 path.
            record_id = warc.record_id()

            text_path = os.path.join(
                self.text_prefix,
                '{}.txt'.format(record_id),
            )

            # Write text.
            text_key = Key(self.text_bucket)
            text_key.key = text_path
            text_key.set_contents_from_string(text)
