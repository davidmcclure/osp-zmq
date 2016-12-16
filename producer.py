

import zmq


def producer():

    context = zmq.Context()

    socket = context.socket(zmq.PUSH)
    socket.bind('tcp://127.0.0.1:5557')

    for i in range(1000):
        socket.send_json(dict(job=i))


producer()
