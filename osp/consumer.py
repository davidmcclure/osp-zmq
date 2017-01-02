

import zmq
import boto
import warc
import io
import magic
import h11
import os

from boto.s3.key import Key

from osp.services import config
from osp import buckets
from osp.utils import html_to_text, pdf_to_text, docx_to_text
from osp.scraper_warc import ScraperWARC


def consumer():

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

            text_key.set_contents_from_string(text)

            print(record_id)

            # dedupe
            # classify syllabus / not syllabus
            # ext institution
            # ext field(s)
            # ext citations

        except Exception as e:
            print(e)
