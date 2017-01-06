

import slate
import docx

from bs4 import BeautifulSoup
from io import BytesIO


def html_to_text(b: bytearray, charset: str='utf8'):
    """Extract text from HTML.
    """
    html = b.decode(charset)

    tree = BeautifulSoup(html, 'html.parser')

    for tag in tree(['script', 'style']):
        tag.extract()

    return tree.get_text()


def pdf_to_text(b: bytearray):
    """Extract text from a PDF.
    """
    return slate.PDF(BytesIO(b)).text()


def docx_to_text(b: bytearray):
    """Extract text from a DOCX.
    """
    document = docx.Document(BytesIO(b))

    return '\n'.join([p.text for p in document.paragraphs])
