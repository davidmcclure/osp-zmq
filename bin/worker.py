

from distributed import Worker
from tornado.ioloop import IOLoop
from threading import Thread

from osp.services import config


loop = IOLoop.current()
t = Thread(target=loop.start, daemon=True)
t.start()

w = Worker(
    config['hosts']['scheduler'],
    config['hosts']['port'],
    loop=loop,
)

w.start(0)
