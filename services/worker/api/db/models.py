from sqlalchemy import Column, String, Text, Boolean, DateTime, func
from sqlalchemy.types import JSON
from worker.api.db.session import Base

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    status = Column(String, nullable=False, default="queued")
    payload = Column(Text, nullable=True)
    result = Column(Text, nullable=True)
    owner = Column(String, nullable=True)  # make nullable for worker-created jobs
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class User(Base):
    __tablename__ = "users"
    username = Column(String, primary_key=True, index=True)
    hashed_password = Column(String, nullable=False)
    email = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)


class UploadSessionModel(Base):
    __tablename__ = "upload_sessions"

    session_id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    files = Column(JSON, nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())