

from sqlalchemy import Column, String, DateTime
from sqlalchemy.sql import func

from .base import Base


class Document(Base):

    __tablename__ = 'document'

    created_at = Column(DateTime, server_default=func.now())

    corpus = Column(String, primary_key=True)

    identifier = Column(String, primary_key=True)

    url = Column(String)
