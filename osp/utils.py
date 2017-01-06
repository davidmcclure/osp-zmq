

import io
import slate
import docx
import pkgutil
import csv

from bs4 import BeautifulSoup


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
    return slate.PDF(io.BytesIO(b)).text()


def docx_to_text(b: bytearray):
    """Extract text from a DOCX.
    """
    document = docx.Document(io.BytesIO(b))

    return '\n'.join([p.text for p in document.paragraphs])


def read_csv(package, path):
    """Read a CSV from package data.

    Args:
        package (str): The package path.
        path (str): The path of the CSV file.

    Returns: csv.DictReader
    """
    data = pkgutil.get_data(package, path).decode('utf8')

    return csv.DictReader(io.StringIO(data))
