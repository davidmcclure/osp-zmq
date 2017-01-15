

from sqlalchemy.ext.declarative import declarative_base

from osp.services import Database


db = Database()

Base = declarative_base()

Base.query = db.session.query_property()
