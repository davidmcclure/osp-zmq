

import slate

from bs4 import BeautifulSoup
from io import BytesIO


def html_to_text(b: bytearray, charset: str='utf8'):
    """Extract text from HTML.
    """
    html = b.decode(charset)

    tree = BeautifulSoup(html, 'html')

    for tag in tree(['script', 'style']):
        tag.extract()

    return tree.get_text()


def pdf_to_text(b: bytearray):
    """Extract text from a PDF.
    """
    return slate.PDF(BytesIO(b)).text()
