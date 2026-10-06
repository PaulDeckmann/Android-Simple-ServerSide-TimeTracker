"""Authentication: password hashing, session/refresh and API tokens, rate limiting."""
import hashlib
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
import bcrypt

from app.config import Config
from app.db import ApiToken, RefreshToken, SessionLocal, User, new_id, utcnow


def hash_password(password: str) -> str:
    pwd_bytes = password.encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    pwd_bytes = password.encode("utf-8")[:72]
    try:
        return bcrypt.checkpw(pwd_bytes, password_hash.encode("utf-8"))
    except Exception:
        return False


def get_secret_key() -> str:
    """Load or create a persistent secret key used for token hashing."""
    path = Config.SECRET_KEY_FILE
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    key = secrets.token_hex(32)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(key)
    return key


SECRET_KEY = get_secret_key()


def token_hash(token: str) -> str:
    return hashlib.sha256((SECRET_KEY + token).encode()).hexdigest()


def create_refresh_token(db: Session, user_id: str) -> str:
    token = secrets.token_urlsafe(48)
    expires_at = utcnow() + timedelta(days=Config.REFRESH_TOKEN_TTL_DAYS)
    db.add(
        RefreshToken(
            id=new_id(),
            user_id=user_id,
            token_hash=token_hash(token),
            expires_at=expires_at,
        ),
    )
    db.commit()
    return token


def create_api_token(db: Session, user_id: str, name: str = "default") -> str:
    token = secrets.token_urlsafe(48)
    expires_at = utcnow() + timedelta(days=Config.API_TOKEN_TTL_DAYS)
    db.add(
        ApiToken(
            id=new_id(),
            user_id=user_id,
            name=name,
            token_hash=token_hash(token),
            expires_at=expires_at,
        ),
    )
    db.commit()
    return token


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _bearer_token(request: Request) -> Optional[str]:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
    return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def authenticate_token(token: Optional[str], db: Session) -> Optional[str]:
    """Return user_id if a valid API token or refresh token string is present."""
    if not token:
        return None
    if token.startswith("Bearer "):
        token = token[len("Bearer "):].strip()
    hashed = token_hash(token)
    api_token = (
        db.query(ApiToken)
        .filter(ApiToken.token_hash == hashed, ApiToken.revoked.is_(False))
        .first()
    )
    if api_token is not None:
        if api_token.expires_at is not None and _aware(api_token.expires_at) < utcnow():
            return None
        api_token.last_used_at = utcnow()
        db.commit()
        return api_token.user_id
    refresh_token = (
        db.query(RefreshToken)
        .filter(RefreshToken.token_hash == hashed, RefreshToken.revoked.is_(False))
        .first()
    )
    if refresh_token is not None and _aware(refresh_token.expires_at) >= utcnow():
        return refresh_token.user_id
    return None


def authenticate_request(request: Request, db: Session) -> Optional[str]:
    """Return user_id if a valid API token or refresh token is present in the request."""
    token = _bearer_token(request)
    return authenticate_token(token, db)


def require_user(request: Request, db: Session = Depends(get_db)) -> str:
    user_id = authenticate_request(request, db)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user_id


class RequestContextMiddleware:
    """In-memory rate limiter for login and API endpoints."""

    def __init__(self, app):
        self.app = app
        self.login_attempts: dict = {}
        self.api_requests: dict = {}
        self.login_limit = 10
        self.login_window = 60
        self.api_limit = 600
        self.api_window = 60

    def _allowed(self, store: dict, key: str, limit: int, window: int) -> bool:
        now = time.monotonic()
        entry = store.get(key)
        if entry is None or now - entry[0] > window:
            store[key] = (now, 1)
            return True
        count = entry[1] + 1
        store[key] = (entry[0], count)
        return count <= limit

    def _client(self, request: Request) -> str:
        return request.client.host if request.client else "unknown"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        path = scope.get("path", "")
        key = self._client(request)
        if path.endswith("/auth/login"):
            if not self._allowed(self.login_attempts, key, self.login_limit, self.login_window):
                response = Response(content=b'{"detail":"Too many requests"}', status_code=429)
                await response(scope, receive, send)
                return
        else:
            if not self._allowed(self.api_requests, key, self.api_limit, self.api_window):
                response = Response(content=b'{"detail":"Too many requests"}', status_code=429)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
