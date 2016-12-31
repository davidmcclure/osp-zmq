

import zmq
import boto


def producer():

    context = zmq.Context()

    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://127.0.0.1:5557')

    conn = boto.connect_s3()
    bucket = conn.get_bucket('syllascrape')

    for key in bucket.list():
        socket.send_string(key.name)


if __name__ == '__main__':
    producer()