

import zmq
import boto
import warc
import io
import h11


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://127.0.0.1:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    while True:

        path = receiver.recv_string()

        key = bucket.get_key(path)

        data = io.BytesIO(key.get_contents_as_string())

        record = warc.WARCReader(data).read_record()

        client = h11.Connection(h11.CLIENT)

        client.receive_data(record.payload.getvalue())

        headers = client.next_event()

        data = client.next_event()

        print(headers)

        # get file type
        # extract text
        # write text


consumer()
