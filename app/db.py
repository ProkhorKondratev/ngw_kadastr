from sqlalchemy import create_engine, Column, JSON, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = "sqlite:///data/database/database.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class DBTask(Base):
    __tablename__ = "ngw_tasks"

    id = Column(Integer, primary_key=True, index=True)
    celery_task = Column(String, unique=True, index=True)
    added = Column(DateTime)
    name = Column(String, index=True)
    kpt_task_id = Column(String, unique=True, index=True)
    kad_task_id = Column(String, unique=True, index=True)
    ngw_resource_id = Column(Integer, unique=True, index=True)

    cover_file = Column(String)
    kpt_file = Column(String)
    kad_file = Column(String)

    kpt_status = Column(JSON, default={"state": "PREPARING"})
    kad_status = Column(JSON, default={"state": "PREPARING"})

    group_id = Column(Integer, ForeignKey("ngw_task_groups.id"))


class DBTasksGroup(Base):
    __tablename__ = "ngw_task_groups"

    id = Column(Integer, primary_key=True, index=True)
    added = Column(DateTime)
    name = Column(String, index=True)


async def create_tables():
    Base.metadata.create_all(bind=engine)


async def drop_tables():
    Base.metadata.drop_all(bind=engine)
