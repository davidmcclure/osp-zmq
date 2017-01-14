

from osp.services import ResultBucket
from osp.scraper_warc import ScraperWARC


def extract_text(path):
    """Extract text, write to S3.
    """
    warc = ScraperWARC.from_s3(path)

    text = warc.text()

    record_id = warc.record_id()

    if text:
        bucket = ResultBucket.Instance()
        bucket.write_text(record_id, text)

    return dict(
        identifier=record_id,
        url=warc.url(),
    )
