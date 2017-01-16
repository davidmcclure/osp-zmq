

from datetime import datetime as dt
from sqlalchemy import Column, String, DateTime, Integer, Boolean
from boltons.iterutils import chunked_iter

from osp.services import db

from .base import Base


class Document(Base):

    __tablename__ = 'document'

    corpus = Column(String, primary_key=True)

    identifier = Column(String, primary_key=True)

    has_text = Column(Boolean, nullable=False)

    url = Column(String)

    downloaded_at = Column(DateTime)

    mime_type = Column(String)

    content_length = Column(Integer)

    @classmethod
    def bulk_insert(cls, mappings, n=1000):
        """Bulk insert rows in pages.

        Args:
            mappings (iter): Rows dicts.
            n (int): Page size.
        """
        for chunk in chunked_iter(mappings, n):

            rows = [r for r in chunk if r is not None]

            db.session.bulk_insert_mappings(cls, rows)
            db.session.commit()

            print(dt.now(), 'store')
