
import os
import secrets
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Dict
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import bcrypt
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from worker.api.db.session import get_session
from worker.api.db.models import User

logger = logging.getLogger(__name__)

# Security Configuration
_jwt_secret = os.getenv("JWT_SECRET")
if not _jwt_secret:
    # Generate a secure random key for development
    _jwt_secret = secrets.token_urlsafe(32)
    logger.warning(
        "JWT_SECRET not set, using generated key. Set JWT_SECRET environment variable for production!"
    )

SECRET_KEY: str = _jwt_secret
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
COLLAB_WEBSOCKET_TOKEN_EXPIRE_SECONDS = int(os.getenv("COLLAB_WEBSOCKET_TOKEN_EXPIRE_SECONDS", "120"))

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
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def get_password_hash(password: str) -> str:
    """Generate password hash."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

async def get_user(db: AsyncSession, username: str) -> Optional[User]:
    """Get user by username."""
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()

async def authenticate_user(db: AsyncSession, username: str, password: str) -> Optional[User]:
    """Authenticate user with username and password."""
    user = await get_user(db, username)
    if not user or not verify_password(password, getattr(user, "hashed_password")):
        return None
    return user

def create_signed_token(data: Dict[str, Any], *, expires_delta: timedelta, token_type: str) -> str:
    """Create a signed JWT token for a specific scope."""
    to_encode: Dict[str, Any] = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire, "type": token_type})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    return create_signed_token(
        data,
        expires_delta=expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )

def create_refresh_token(data: Dict[str, Any]) -> str:
    """Create JWT refresh token."""
    return create_signed_token(
        data,
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        token_type="refresh",
    )

def create_collab_websocket_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Create a short-lived token scoped to collaboration websocket auth."""
    return create_signed_token(
        data,
        expires_delta=expires_delta or timedelta(seconds=COLLAB_WEBSOCKET_TOKEN_EXPIRE_SECONDS),
        token_type="collab_ws",
    )

def decode_token(token: str) -> Dict[str, Any]:
    """Decode and validate JWT token."""
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        logger.error("Token decode error: %s", e)
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Get current authenticated user from JWT token."""
    try:
        payload = decode_token(credentials.credentials)
        username_raw = payload.get("sub")
        token_type = payload.get("type")

        if username_raw is None or token_type != "access":
            raise HTTPException(status_code=401, detail="Invalid credentials")

        username: str = str(username_raw)

        user = await get_user(session, username)
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        return user

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

async def get_current_active_user(user: User = Depends(get_current_user)) -> User:
    if not bool(getattr(user, "is_active", True)):
        raise HTTPException(status_code=400, detail="Inactive user")
    return user

async def get_current_admin(user: User = Depends(get_current_active_user)) -> User:
    if not bool(getattr(user, "is_admin", False)):
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return user

"""Authentication utilities (password hashing, token creation, dependencies).

HTTP routes are defined in routes/auth_routes.py to avoid duplication and
keep this module focused on reusable helpers.
"""
