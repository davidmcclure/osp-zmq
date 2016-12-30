

import zmq
import boto
import warc
import io
import magic
import h11
import os

from boto.s3.key import Key

from utils import html_to_text, pdf_to_text


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://127.0.0.1:5557')

    conn = boto.connect_s3()
    warc_bucket = conn.get_bucket('syllascrape')
    text_bucket = conn.get_bucket('osp-pipeline-test')

    while True:

        try:

            # Parse the WARC.

            warc_path = receiver.recv_string()

            warc_key = warc_bucket.get_key(warc_path)

            contents = io.BytesIO(warc_key.get_contents_as_string())

            record = warc.WARCReader(contents).read_record()

            client = h11.Connection(h11.CLIENT)

            client.receive_data(record.payload.getvalue())

            headers = client.next_event()

            data = client.next_event()

            if data == h11.NEED_DATA:
                continue

            # Extract text.

            mime = magic.from_buffer(bytes(data.data), mime=True)

            text = None

            if mime == 'text/html':
                text = html_to_text(data.data)

            elif mime == 'application/pdf':
                text = pdf_to_text(data.data)

            # TODO: docx

            if not text:
                continue

            # Write text.

            path, _ = os.path.splitext(warc_path)

            record_id = os.path.basename(path)

            text_path = os.path.join('zmq', '{}.txt'.format(record_id))

            text_key = Key(text_bucket)
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


if __name__ == '__main__':
    consumer()
