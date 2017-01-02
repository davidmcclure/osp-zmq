

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
from osp import buckets


def ventilator():

    context = zmq.Context()

    sender = context.socket(zmq.PUSH)
    sender.bind('tcp://*:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    for i, key in enumerate(bucket.list()):

        sender.send_string(key.name)

        if i % 1000 == 0:
            print(dt.now().isoformat(), i)


def worker():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://{}:5557'.format(config['zmq_host']))

    while True:

        try:

            # Parse the WARC.

            warc_path = receiver.recv_string()

            warc = ScraperWARC.from_s3(warc_path)

            # Extract text.

            text = warc.text()

            if not text:
                continue

            # Write text.

            record_id = warc.record_id()

            text_path = os.path.join('zmq3', '{}.txt'.format(record_id))

            text_key = Key(buckets.texts)
            text_key.key = text_path

            # text_key.set_contents_from_string(text)
            print(record_id)

        except Exception as e:
            print(e)


class Job:

    def __init__(self, ventilator, worker):
        self.ventilator = ventilator
        self.worker = worker

    def run_local(self, n=2):
        """Spawn local procs for the ventilator and worker.
        """
        # Run ventilator.
        Process(target=ventilator).start()

        # Run workers.
        for _ in range(n):
            Process(target=worker).start()
