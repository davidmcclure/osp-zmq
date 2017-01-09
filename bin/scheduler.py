

from distributed import Scheduler
from tornado.ioloop import IOLoop
from threading import Thread

from osp.services import config


loop = IOLoop.current()
t = Thread(target=loop.start, daemon=True)
t.start()

s = Scheduler(loop=loop)
s.start(config['ports']['scheduler'])
