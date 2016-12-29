

import re

from bs4 import BeautifulSoup


def html_to_text(b: bytearray, charset: str='utf8'):
    """Extract text from an HTML response.
    """
    html = b.decode(charset)

    tree = BeautifulSoup(html, 'html')

    for tag in tree(['script', 'style']):
        tag.extract()

    return tree.get_text()


def pdf_to_text(b):
    pass


def docx_to_text(b):
    pass
