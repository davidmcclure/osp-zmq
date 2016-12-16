

import zmq


def consumer():

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://127.0.0.1:5557')

    while True:
        work = receiver.recv_string()
        print(work)


consumer()
