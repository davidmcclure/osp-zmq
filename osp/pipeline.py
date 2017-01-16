

from osp.services import result_bucket
from osp.scraper_warc import ScraperWARC


def extract_text(path):
    """Extract text, write to S3.

    Args:
        path (str): S3 path for WARC.

    Returns:
        dict: Document metadata.
    """
    warc = ScraperWARC.from_s3(path)

    text = None

    # TODO: Do this at the map level?
    try:
        text = warc.text()
    except Exception as e:
        print(e)

    record_id = warc.record_id()

    if text:
        result_bucket.write_text(record_id, text)

    # TODO: Where to parametrize the corpus?
    return dict(
        corpus='test',
        identifier=record_id,
        url=warc.url(),
        downloaded_at=warc.date(),
        mime_type=warc.mime_type(),
        content_length=warc.content_length(),
    )
