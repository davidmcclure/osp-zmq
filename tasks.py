

from invoke import task

from osp.services import db
from osp.models import Base


@task
def init_db(ctx):
    """Create database tables.
    """
    Base.metadata.create_all(db.engine)


@task
def reset_db(ctx):
    """Drop and recreate database tables.
    """
    Base.metadata.drop_all(db.engine)
    init_db(ctx)
