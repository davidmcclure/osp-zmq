

import zmq
import boto


def producer():

    context = zmq.Context()

    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://*:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    for key in bucket.list():
        socket.send_string(key.name)
