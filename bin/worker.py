

from distributed import Worker
from tornado.ioloop import IOLoop
from threading import Thread

from osp import settings


loop = IOLoop.current()
t = Thread(target=loop.start, daemon=True)
t.start()

w = Worker(
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
    loop=loop,
)

w.start(0)
