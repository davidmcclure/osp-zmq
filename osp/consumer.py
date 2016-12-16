

import zmq
import boto


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://127.0.0.1:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    while True:

        path = receiver.recv_string()

        key = bucket.get_key(path)

        data = key.get_contents_as_string()

        # read warc
        # get file type
        # extract text
        # write text

        print(path)


consumer()
