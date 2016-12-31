

from .services import s3


# TODO: ENV-ify.
warcs = s3.get_bucket('syllascrape')
