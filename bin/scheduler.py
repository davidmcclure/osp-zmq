

from distributed import Scheduler
from tornado.ioloop import IOLoop
from threading import Thread

from osp import settings


loop = IOLoop.current()
t = Thread(target=loop.start, daemon=True)
t.start()

s = Scheduler(loop=loop)
s.start(settings.SCHEDULER_PORT)
