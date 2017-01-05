

import os
import io
import zmq
import ujson
import boto
import uuid

from boto.s3.key import Key

from osp.services import config, session
from osp.scraper_warc import ScraperWARC
from osp.models import Document


class Ventilator:

    @classmethod
    def from_env(cls):
        return cls(config['ports']['ventilator'])

    def __init__(self, port):
        """Initialize the push socket.
        """
        context = zmq.Context()

        self.socket = context.socket(zmq.PUSH)
        self.socket.bind('tcp://*:{}'.format(port))

    def __call__(self, tasks):
        """Broadcast tasks.

        Args:
            tasks (func): A function that generates tasks.
        """
        for task in tasks():
            self.socket.send_json(task)


class Worker:

    @classmethod
    def from_env(cls):
        return cls(
            config['hosts']['master'],
            config['ports']['ventilator'],
        )

    def __init__(self, host, port):
        """Initialize the sockets.

        Args:
            host (str): The master host.
            port (int): Ventilator port.
        """
        context = zmq.Context()

        self.ventilator = context.socket(zmq.PULL)
        self.ventilator.connect('tcp://{}:{}'.format(host, port))

    def __call__(self, work):
        """Pull tasks from ventilator, push results to sink.

        Args:
            work (func): A worker function.
        """
        while True:

            try:
                task = self.ventilator.recv_json()
                work(**task)

            except Exception as e:
                print(e)


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
            config['dirs']['document'],
        )

    def __init__(self, warc_bucket, text_bucket, text_dir, document_dir):
        """Set input + output buckets.

        Args:
            warc_bucket (str): Scraper bucket.
            text_bucket (str): Text bucket.
            text_dir (str): Path prefix for extracted text.
            document_dir (str): Path prefix for documents.
        """
        s3 = boto.connect_s3()

        self.warc_bucket = s3.get_bucket(warc_bucket)
        self.text_bucket = s3.get_bucket(text_bucket)

        self.text_dir = text_dir
        self.document_dir = document_dir

        self.results = []

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

        # Register metadata.
        self.results.append(dict(
            corpus='syllascrape',
            identifier=record_id,
            url=warc.url(),
        ))

        if len(self.results) >= 1000:
            self.flush()

    def flush(self):
        """Flush results to s3.
        """
        docs = ujson.dumps(self.results)

        # Form S3 path.
        path = os.path.join(
            self.document_dir,
            '{}.json'.format(str(uuid.uuid4())),
        )

        # Write segment.
        docs_key = Key(self.text_bucket)
        docs_key.key = path
        docs_key.set_contents_from_string(docs)

        self.results.clear()
