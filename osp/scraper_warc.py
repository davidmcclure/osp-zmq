

import warc
import h11
import magic
import io

from osp.utils import html_to_text, pdf_to_text, docx_to_text
from osp.services import ScraperBucket


class ScraperWARC:

    @classmethod
    def from_s3(cls, path):
        """Read from S3.
        """
        bucket = ScraperBucket.Instance()

        blob = bucket.read_bytes(path)

        return cls(blob)

    def __init__(self, blob):
        """Parse the binary WARC data.

        Args:
            blob: The WARC, as a binary blob.
        """
        self.record = warc.WARCReader(blob).read_record()

        client = h11.Connection(h11.CLIENT)

        client.receive_data(self.record.payload.getvalue())

        self.headers = client.next_event()

        self.data = client.next_event()

    def mime_type(self):
        """Try to parse the MIME type of the response.

        Returns: str or None
        """
        if self.data == h11.NEED_DATA:
            return None

        return magic.from_buffer(bytes(self.data.data), mime=True)

    def text(self):
        """Extract plain text.

        Returns: str or None
        """

        mime = self.mime_type()

        if mime == 'text/html':
            return html_to_text(self.data.data)

        elif mime == 'application/pdf':
            return pdf_to_text(self.data.data)

        elif mime == 'application/msword':
            return docx_to_text(self.data.data)

    def record_id(self):
        """Provide the record UUID.

        Returns: str
        """
        return self.record.header.record_id.strip('<>').split(':')[-1]

    def url(self):
        """Provide the source URL.

        Returns: str
        """
        return self.record.url
