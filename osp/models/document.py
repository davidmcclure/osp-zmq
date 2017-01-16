

from boltons.iterutils import chunked_iter
from sqlalchemy import Column, String, DateTime

from osp.services import Database

from .base import Base


class Document(Base):

    __tablename__ = 'document'

    corpus = Column(String, primary_key=True)

    identifier = Column(String, primary_key=True)

    url = Column(String)

    retrieved = Column(DateTime)

    mime_type = Column(String)

    # TODO: Move to base class.
    # TODO: Pass 'corpus' param?
    @classmethod
    def bulk_insert(cls, mappings, n=1000):
        """Bulk insert rows in pages.

        Args:
            mappings (iter): Rows dicts.
            n (int): Page size.
        """
        # TODO: Global singleton.
        db = Database()

        for chunk in chunked_iter(mappings, n):
            db.session.bulk_insert_mappings(cls, chunk)

        db.session.commit()
