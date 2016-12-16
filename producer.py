

import zmq

from boto.s3.connection import S3Connection


def producer():

    context = zmq.Context()

    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://127.0.0.1:5557')

    conn = S3Connection()

    bucket = conn.get_bucket('syllascrape')

    for key in bucket.list():
        socket.send_string(key.name)


producer()
