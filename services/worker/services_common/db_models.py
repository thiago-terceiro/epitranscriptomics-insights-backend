from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Text, DateTime, func
from sqlalchemy import create_engine
from .db_session import DATABASE_URL

Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    status = Column(String, nullable=False, default="queued")
    payload = Column(Text, nullable=True)
    result = Column(Text, nullable=True)
    owner = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
