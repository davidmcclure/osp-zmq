

import boto

from osp.config import Config


config = Config.from_env()

s3 = boto.connect_s3()
