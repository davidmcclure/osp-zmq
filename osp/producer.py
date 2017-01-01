

import zmq
import boto

from datetime import datetime as dt


def producer():

    context = zmq.Context()

    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://*:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    for i, key in enumerate(bucket.list()):

        socket.send_string(key.name)

        if i % 1000 == 0:
            print(dt.now().isoformat(), i)
