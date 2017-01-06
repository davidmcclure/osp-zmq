

from sqlalchemy import Column, String, DateTime

from .base import Base


class Document(Base):

    __tablename__ = 'document'

    corpus = Column(String, primary_key=True)

    identifier = Column(String, primary_key=True)

    url = Column(String)

    retrieved = Column(DateTime)

    mime_type = Column(String)
