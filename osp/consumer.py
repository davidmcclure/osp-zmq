

import zmq
import boto
import warc
import io
import magic
import h11

from utils import html_to_text


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://127.0.0.1:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    while True:

        path = receiver.recv_string()

        key = bucket.get_key(path)

        contents = io.BytesIO(key.get_contents_as_string())

        record = warc.WARCReader(contents).read_record()

        client = h11.Connection(h11.CLIENT)

        client.receive_data(record.payload.getvalue())

        headers = client.next_event()

        data = client.next_event()

        if data == h11.NEED_DATA:
            continue

        mime = magic.from_buffer(bytes(data.data), mime=True)

        if mime == 'text/html':
            text = html_to_text(data.data)
            print(text)

        # extract text
        # write text


consumer()
