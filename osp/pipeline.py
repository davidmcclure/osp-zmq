

import zmq
import boto
import io
import os

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
            config['ports']['sink'],
        )

    def __init__(self, host, pull_port, push_port):
        """Initialize the sockets.

        Args:
            host (str): The master host.
            pull_port (int): Ventilator port.
            push_port (int): Sink port.
        """
        context = zmq.Context()

        self.ventilator = context.socket(zmq.PULL)
        self.ventilator.connect('tcp://{}:{}'.format(host, pull_port))

        self.sink = context.socket(zmq.PUSH)
        self.sink.connect('tcp://{}:{}'.format(host, push_port))

    def __call__(self, work):
        """Pull tasks from ventilator, push results to sink.

        Args:
            work (func): A worker function.
        """
        while True:

            try:

                # Pop task, process.
                task = self.ventilator.recv_json()
                result = work(**task)

                # If a value is returned, push it back to the master.
                if result:
                    self.sink.send_json(result)

            except Exception as e:
                print(e)


class Sink:

    @classmethod
    def from_env(cls):
        return cls(config['ports']['sink'])

    def __init__(self, port):
        """Initialize the socket.

        Args:
            port (int): Sink port.
        """
        context = zmq.Context()

        self.socket = context.socket(zmq.PULL)
        self.socket.bind('tcp://*:{}'.format(port))

        self.results = []

    def __call__(self, drain):
        """Pull tasks from workers.

        Args:
            drain (func): Handle / persist results.
        """
        while True:

            # TODO: Pass chunks?

            try:
                result = self.socket.recv_json()
                self.results.append(result)

            except Exception as e:
                print(e)

            if len(self.results) >= 1000:
                drain(self.results)
                self.results.clear()


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
            text_dir (str): Path prefix inside of text bucket.
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

        # TODO: Get metadata.
        return dict(
            corpus='syllascrape',
            identifier=record_id,
            url=warc.url(),
        )


# TODO|dev
def write_doc_metadata(docs):
    """Write document metadata to database.
    """
    session.bulk_insert_mappings(Document, docs)
    session.commit()
