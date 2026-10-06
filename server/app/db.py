"""Database setup: engine, session, and ORM models.

Single-user server. All entities use UUID strings as IDs and
`updated_at` timestamps for conservative last-writer-wins sync.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import Config

if Config.DATABASE_URL == "sqlite://":
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
else:
    engine = create_engine(Config.DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=new_id)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    token_hash = Column(String, unique=True, nullable=False)
    revoked = Column(Boolean, default=False, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class ApiToken(Base):
    __tablename__ = "api_tokens"

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String, default="default", nullable=False)
    token_hash = Column(String, unique=True, nullable=False)
    revoked = Column(Boolean, default=False, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class Activity(Base):
    __tablename__ = "activities"
    __table_args__ = (
        Index("ix_activities_updated_at", "updated_at"),
        Index("ix_activities_user_updated", "user_id", "updated_at"),
    )

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    color = Column(String, default="", nullable=False)
    color_id = Column(Integer, default=0, nullable=False)
    icon = Column(String, default="", nullable=False)
    sort_order = Column(Integer, default=0, nullable=False)
    archived = Column(Boolean, default=False, nullable=False)
    parent_activity_id = Column(String, ForeignKey("activities.id"), nullable=True)
    category = Column(String, default="", nullable=False)
    goal_seconds_per_week = Column(Integer, nullable=True)
    goal_seconds_total = Column(Integer, nullable=True)
    goal_days_per_month = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class TimeEntry(Base):
    __tablename__ = "time_entries"
    __table_args__ = (
        Index("ix_time_entries_updated_at", "updated_at"),
        Index("ix_time_entries_user_started", "user_id", "started_at"),
        Index("ix_time_entries_user_updated", "user_id", "updated_at"),
        Index("ix_time_entries_user_running", "user_id", "ended_at", "deleted_at"),
    )

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, nullable=False, index=True)
    activity_id = Column(String, ForeignKey("activities.id"), nullable=False)
    parent_activity_ids = Column(String, default="", nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, default=0, nullable=False)
    comment = Column(Text, default="", nullable=False)
    tags = Column(Text, default="", nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    sync_status = Column(String, default="synced", nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class Goal(Base):
    __tablename__ = "goals"
    __table_args__ = (
        Index("ix_goals_updated_at", "updated_at"),
        Index("ix_goals_user_updated", "user_id", "updated_at"),
    )

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, nullable=False, index=True)
    activity_id = Column(String, ForeignKey("activities.id"), nullable=False)
    goal_type = Column(String, default="seconds_per_week", nullable=False)
    goal_value = Column(Float, default=0, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class SyncCategory(Base):
    """Generic sync storage for categories; data holds the app owned json payload."""

    __tablename__ = "sync_categories"
    __table_args__ = (
        Index("ix_sync_categories_user_updated", "user_id", "updated_at"),
    )

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class SyncTag(Base):
    """Generic sync storage for record tags; data holds the app owned json payload."""

    __tablename__ = "sync_tags"
    __table_args__ = (
        Index("ix_sync_tags_user_updated", "user_id", "updated_at"),
    )

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class SyncTimetable(Base):
    """Generic sync storage for timetable entities; data holds the app owned json payload."""

    __tablename__ = "sync_timetable"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    entity_type = Column(String, nullable=False, index=True)
    data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_sync_timetable_updated_at", "updated_at"),
        Index("ix_sync_timetable_type_id", "entity_type", "id"),
        Index("ix_sync_timetable_user_updated", "user_id", "updated_at"),
        Index("ix_sync_timetable_user_type", "user_id", "entity_type"),
    )


class SyncLog(Base):
    __tablename__ = "sync_log"

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, nullable=False, index=True)
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False)
    action = Column(String, nullable=False)
    detail = Column(Text, default="", nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class ConflictLog(Base):
    __tablename__ = "conflict_log"

    id = Column(String, primary_key=True, default=new_id)
    user_id = Column(String, nullable=False, index=True)
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False)
    resolution = Column(String, nullable=False)
    detail = Column(Text, default="", nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


def init_db() -> None:
    """Create tables. In production, Alembic migrations are used instead."""
    import app.models  # noqa: F401  ensure models are registered

    Base.metadata.create_all(bind=engine)
