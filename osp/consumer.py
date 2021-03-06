

import zmq
import boto
import warc
import io
import magic
import h11
import os

from boto.s3.key import Key

from osp.services import config
from osp.utils import html_to_text, pdf_to_text, docx_to_text
from osp import buckets


class Response:

    @classmethod
    def from_s3(cls, path):
        """Read from a S3 key.

        Args:
            path (str): The s3 WARC path.
        """
        key = buckets.warcs.get_key(path)

        blob = io.BytesIO(key.get_contents_as_string())

        return cls(blob)

    def __init__(self, blob):
        """Parse the binary WARC data.

        Args:
            blob: The WARC, as a binary blob.
        """
        self.record = warc.WARCReader(blob).read_record()

        client = h11.Connection(h11.CLIENT)

        client.receive_data(self.record.payload.getvalue())

        self.headers = client.next_event()

        self.data = client.next_event()

    def text(self):
        """Extract plain text.

        Returns: str
        """
        if self.data == h11.NEED_DATA:
            return None

        mime = magic.from_buffer(bytes(self.data.data), mime=True)
        print(mime)

        if mime == 'text/html':
            return html_to_text(self.data.data)

        elif mime == 'application/pdf':
            return pdf_to_text(self.data.data)

        elif mime == 'application/msword':
            return docx_to_text(self.data.data)

    def record_id(self):
        """Provide the record UUID.

        Returns: str
        """
        return self.record.header.record_id.strip('<>').split(':')[-1]


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)

    receiver.connect('tcp://{}:5557'.format(config['zmq_host']))

    while True:

        try:

            # Parse the WARC.

            warc_path = receiver.recv_string()

            response = Response.from_s3(warc_path)

            # Extract text.

            text = response.text()

            if not text:
                continue

            # TODO|dev
            # Write text.

            record_id = response.record_id()

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
