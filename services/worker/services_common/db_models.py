from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Text, DateTime, func
from sqlalchemy.types import JSON

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


class UploadSessionModel(Base):
    __tablename__ = "upload_sessions"

    session_id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    files = Column(JSON, nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
