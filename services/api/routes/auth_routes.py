from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta
from ..db.session import get_session
from ..auth import (
    authenticate_user,
    create_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    get_password_hash,
)
from ..db.models import User
from ..schemas import UserCreate, TokenResponse

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/token", response_model=TokenResponse)
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: Session = Depends(get_session),
):
    user = authenticate_user(session, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return TokenResponse(access_token=access_token)

@router.post("/register")
def register_user(
    body: UserCreate,
    session: Session = Depends(get_session),
):
    existing = session.get(User, body.username)
    if existing:
        raise HTTPException(status_code=400, detail="User exists")
    hashed = get_password_hash(body.password)
    user = User(
        username=body.username,
        hashed_password=hashed,
        email=body.email,
        is_active=True,
    )
    session.add(user)
    session.commit()
    return {"username": body.username}
