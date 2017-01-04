

import warc
import h11
import magic

from osp.utils import html_to_text, pdf_to_text, docx_to_text


class ScraperWARC:

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

    def url(self):
        """Provide the source URL.

        Returns: str
        """
        return self.record.url
