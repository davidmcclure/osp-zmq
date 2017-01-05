

from sqlalchemy import Column, String

from .base import Base


class Document(Base):

    __tablename__ = 'document'

    corpus = Column(String, primary_key=True)

    identifier = Column(String, primary_key=True)

    url = Column(String)
