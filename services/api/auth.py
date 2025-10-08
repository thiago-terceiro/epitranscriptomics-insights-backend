
import os
import secrets
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Dict
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .db.session import SessionLocal
from .db.models import User

logger = logging.getLogger(__name__)

# Security Configuration
_SECRET_KEY = os.getenv("JWT_SECRET")
if not _SECRET_KEY:
    # Generate a secure random key for development
    _SECRET_KEY = secrets.token_urlsafe(32)
    logger.warning(
        "JWT_SECRET not set, using generated key. Set JWT_SECRET environment variable for production!"
    )

SECRET_KEY: str = _SECRET_KEY
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# Password Configuration
pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__rounds=12  # Increased for better security
)

# Bearer token scheme
security = HTTPBearer()

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int

class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: list[str] = []

class RefreshTokenRequest(BaseModel):
    refresh_token: str

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Generate password hash."""
    return pwd_context.hash(password)

def get_user(db: Session, username: str) -> Optional[User]:
    """Get user by username."""
    return db.query(User).filter(User.username == username).first()

def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    """Authenticate user with username and password."""
    user = get_user(db, username)
    if not user or not verify_password(password, getattr(user, "hashed_password")):
        return None
    return user

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode: Dict[str, Any] = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: Dict[str, Any]) -> str:
    """Create JWT refresh token."""
    expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode: Dict[str, Any] = {**data, "exp": expire, "type": "refresh"}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Dict[str, Any]:
    """Decode and validate JWT token."""
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        logger.error(f"Token decode error: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> User:
    """Get current authenticated user from JWT token."""
    try:
        payload = decode_token(credentials.credentials)
        username_raw = payload.get("sub")
        token_type = payload.get("type")
        
        if username_raw is None or token_type != "access":
            raise HTTPException(status_code=401, detail="Invalid credentials")
            
        username: str = str(username_raw)
        
        # Get user from database
        db = SessionLocal()
        try:
            user = get_user(db, username)
            if user is None:
                raise HTTPException(status_code=401, detail="User not found")
            return user
        finally:
            db.close()
            
    except JWTError:
        raise HTTPException(status_code=401, detail="Could not validate credentials")

def verify_refresh_token(token: str) -> str:
    """Verify refresh token and return username."""
    try:
        payload = decode_token(token)
        username = payload.get("sub")
        token_type = payload.get("type")
        
        if username is None or token_type != "refresh":
            raise HTTPException(status_code=401, detail="Invalid refresh token")
            
        return str(username)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    db = SessionLocal()
    user = get_user(db, username=username)
    db.close()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

def get_current_active_user(user: User = Depends(get_current_user)) -> User:
    if not bool(getattr(user, "is_active", True)):
        raise HTTPException(status_code=400, detail="Inactive user")
    return user

def get_current_admin(user: User = Depends(get_current_active_user)) -> User:
    if not bool(getattr(user, "is_admin", False)):
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return user

"""Authentication utilities (password hashing, token creation, dependencies).

HTTP routes are defined in routes/auth_routes.py to avoid duplication and
keep this module focused on reusable helpers.
"""
