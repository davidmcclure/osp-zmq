

import zmq
import boto
import warc
import io
import magic
import h11
import os

from datetime import datetime as dt
from boto.s3.key import Key
from multiprocessing import Process

from osp.services import config
from osp.scraper_warc import ScraperWARC


class Ventilator:

    def __init__(self, port):
        """Initialize the push socket.
        """
        context = zmq.Context()

        self.sender = context.socket(zmq.PUSH)
        self.sender.bind('tcp://*:{}'.format(port))

    def __call__(self):
        """Broadcast tasks.
        """
        for task in self.tasks():
            self.sender.send_string(task)

    def tasks(self):
        raise NotImplementedError


class Worker:

    def __init__(self, host, port):
        """Initialize the pull socket.
        """
        context = zmq.Context()

        self.receiver = context.socket(zmq.PULL)
        self.receiver.connect('tcp://{}:{}'.format(host, port))

    def __call__(self):
        """Pull tasks from ventilator.
        """
        while True:

            try:
                task = self.receiver.recv_string()
                self.process(task)
                print(task)

            except Exception as e:
                print(e)

    def process(self, task):
        raise NotImplementedError


class CorpusVentilator:

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


class CorpusWorker:

    def __init__(self, bucket, prefix):
        """Set output bucket and path prefix.

        Args:
            bucket (str): Name of output bucket.
            prefix (str): Path prefix inside of bucket.
        """
        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(bucket)
        self.prefix = prefix

    def __call__(self, warc_path):
        """Extract text, write to S3.

        Args:
            warc_path (str): WARC S3 path.
        """
        # Extract text.
        warc = ScraperWARC.from_s3(warc_path)
        text = warc.text()

        if text:

            # Form S3 path.
            record_id = warc.record_id()
            text_path = os.path.join(self.prefix, '{}.txt'.format(record_id))

            # Write text.
            text_key = Key(self.buckets)
            text_key.key = text_path
            text_key.set_contents_from_string(text)
