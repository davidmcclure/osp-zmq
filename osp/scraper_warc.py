

import io
import warc
import h11
import magic

from osp import buckets


class ScraperWARC:

    @classmethod
    def from_s3(cls, path):
        """Read from a S3 key.

        Args:
            path (str): The s3 WARC path.
        """
        key = buckets.warcs.get_key(path)

        blob = io.BytesIO(key.get_contents_as_string())

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

    def text(self):
        """Extract plain text.

        Returns: str
        """
        if self.data == h11.NEED_DATA:
            return None

        mime = magic.from_buffer(bytes(self.data.data), mime=True)

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
