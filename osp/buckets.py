

from .services import s3


# TODO: ENV-ify.
warcs = s3.get_bucket('syllascrape')
texts = s3.get_bucket('osp-pipeline-test')
