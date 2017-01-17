

from osp.services import result_bucket, scraper_bucket
from osp.scraper_warc import ScraperWARC


# TODO: Abstract Job class?


def try_except(fn):
    """Swallow exceptions.
    """
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(e)

    return wrapped


class ExtractText:

    def __init__(self):
        self.counter = 0

    def __call__(self, path):
        """Extract text, write to S3.

        Args:
            path (str): S3 path for WARC.

        Returns:
            dict: Document metadata.
        """
        warc = ScraperWARC.from_s3(path)

        text = warc.text()

        record_id = warc.record_id()

        if text:
            result_bucket.write_text(record_id, text)

        self.counter += 1

        # TODO: Wrap this up in a base S3 class?
        if self.counter % 80 == 0:
            scraper_bucket.bucket.connection.close()
            result_bucket.bucket.connection.close()

        # TODO: Parametrize corpus.
        return dict(
            corpus='test',
            identifier=record_id,
            has_text=bool(text),
            url=warc.url(),
            downloaded_at=warc.date(),
            mime_type=warc.mime_type(),
            content_length=warc.content_length(),
        )
