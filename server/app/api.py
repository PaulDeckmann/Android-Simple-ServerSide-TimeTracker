"""API routes for the time tracker server."""
import csv
import difflib
import io
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import Config
from app.db import (
    Activity,
    ConflictLog,
    Goal,
    SessionLocal,
    SyncCategory,
    SyncLog,
    SyncTag,
    SyncTimetable,
    TimeEntry,
    User,
    new_id,
    utcnow,
)
from app.events import manager
from app.mqtt import (
    MQTT_TOPIC_PREFIX,
    get_ha_device_info,
    get_ha_discovery_payloads,
    publish_discovery_and_state,
)
from app.security import (
    authenticate_token,
    create_api_token,
    create_refresh_token,
    get_db,
    hash_password,
    require_user,
    token_hash,
    verify_password,
)

api_router = APIRouter()


def iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()

def _aware(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def parse_dt(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid datetime: {value}") from exc


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


@api_router.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == body.username).first()
    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    refresh = create_refresh_token(db, user.id)
    api = create_api_token(db, user.id)
    return TokenResponse(
        access_token=api,
        refresh_token=refresh,
        expires_in=Config.API_TOKEN_TTL_DAYS * 86400,
    )


class RefreshRequest(BaseModel):
    refresh_token: str


@api_router.post("/auth/refresh", response_model=TokenResponse)
def refresh(body: RefreshRequest, db: Session = Depends(get_db)):
    from app.db import ApiToken, RefreshToken

    hashed = token_hash(body.refresh_token)
    token_row = (
        db.query(RefreshToken)
        .filter(RefreshToken.token_hash == hashed, RefreshToken.revoked.is_(False))
        .first()
    )
    if token_row is None or _aware(token_row.expires_at) < utcnow():
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    api = create_api_token(db, token_row.user_id)
    return TokenResponse(
        access_token=api,
        refresh_token=body.refresh_token,
        expires_in=Config.API_TOKEN_TTL_DAYS * 86400,
    )


@api_router.post("/auth/logout")
def logout(request: Request, db: Session = Depends(get_db)):
    from app.security import _bearer_token  # noqa: PLC2701

    token = _bearer_token(request)
    if token:
        hashed = token_hash(token)
        from app.db import ApiToken, RefreshToken

        for table in (ApiToken, RefreshToken):
            row = db.query(table).filter(table.token_hash == hashed).first()
            if row is not None:
                row.revoked = True
                db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Setup (single user bootstrap; only possible when no user exists)
# ---------------------------------------------------------------------------

class SetupRequest(BaseModel):
    username: str
    password: str = Field(min_length=8)


@api_router.post("/auth/setup")
def setup(body: SetupRequest, db: Session = Depends(get_db)):
    if db.query(User).count() > 0:
        raise HTTPException(status_code=400, detail="User already exists")
    user = User(
        id=new_id(),
        username=body.username,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

class ActivityIn(BaseModel):
    id: Optional[str] = None
    name: str
    color: str = ""
    color_id: int = 0
    icon: str = ""
    sort_order: int = 0
    archived: bool = False
    parent_activity_id: Optional[str] = None
    category: str = ""
    goal_seconds_per_week: Optional[int] = None
    goal_seconds_total: Optional[int] = None
    goal_days_per_month: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    deleted_at: Optional[str] = None


def activity_out(a: Activity) -> dict:
    return {
        "id": a.id,
        "name": a.name,
        "color": a.color,
        "color_id": a.color_id,
        "icon": a.icon,
        "sort_order": a.sort_order,
        "archived": a.archived,
        "parent_activity_id": a.parent_activity_id,
        "category": a.category,
        "goal_seconds_per_week": a.goal_seconds_per_week,
        "goal_seconds_total": a.goal_seconds_total,
        "goal_days_per_month": a.goal_days_per_month,
        "created_at": iso(a.created_at),
        "updated_at": iso(a.updated_at),
        "deleted_at": iso(a.deleted_at),
    }


@api_router.get("/activities")
def list_activities(
    include_archived: bool = False,
    db: Session = Depends(get_db),
    user_id: str = Depends(require_user),
):
    query = db.query(Activity).filter(Activity.user_id == user_id)
    if not include_archived:
        query = query.filter(Activity.archived.is_(False), Activity.deleted_at.is_(None))
    return [activity_out(a) for a in query.order_by(Activity.sort_order).all()]


@api_router.post("/activities")
def create_activity(body: ActivityIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activity = Activity(
        id=body.id or new_id(),
        user_id=user_id,
        name=body.name,
        color=body.color,
        color_id=body.color_id,
        icon=body.icon,
        sort_order=body.sort_order,
        archived=body.archived,
        parent_activity_id=body.parent_activity_id,
        category=body.category,
        goal_seconds_per_week=body.goal_seconds_per_week,
        goal_seconds_total=body.goal_seconds_total,
        goal_days_per_month=body.goal_days_per_month,
        created_at=parse_dt(body.created_at) if body.created_at else utcnow(),
        updated_at=parse_dt(body.updated_at) if body.updated_at else utcnow(),
        deleted_at=parse_dt(body.deleted_at) if body.deleted_at else None,
    )
    db.add(activity)
    db.commit()
    db.add(SyncLog(id=new_id(), user_id=user_id, entity_type="activity", entity_id=activity.id, action="created"))
    db.commit()
    return activity_out(activity)


@api_router.patch("/activities/{activity_id}")
def update_activity(activity_id: str, body: ActivityIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activity = db.query(Activity).filter(Activity.id == activity_id, Activity.user_id == user_id).first()
    if activity is None:
        raise HTTPException(status_code=404, detail="Activity not found")
    incoming_updated = parse_dt(body.updated_at) if body.updated_at else utcnow()
    if incoming_updated < _aware(activity.updated_at):
        db.add(
            ConflictLog(
                id=new_id(),
                user_id=user_id,
                entity_type="activity",
                entity_id=activity_id,
                resolution="server_kept_newer",
                detail=f"server_updated_at={iso(activity.updated_at)} incoming_updated_at={body.updated_at}",
            ),
        )
        db.commit()
        return activity_out(activity)
    for field in (
        "name", "color", "color_id", "icon", "sort_order", "archived", "parent_activity_id",
        "category", "goal_seconds_per_week", "goal_seconds_total", "goal_days_per_month",
    ):
        setattr(activity, field, getattr(body, field))
    activity.updated_at = incoming_updated
    db.commit()
    return activity_out(activity)


@api_router.post("/activities/{activity_id}/archive")
def archive_activity(activity_id: str, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activity = db.query(Activity).filter(Activity.id == activity_id, Activity.user_id == user_id).first()
    if activity is None:
        raise HTTPException(status_code=404, detail="Activity not found")
    activity.archived = True
    activity.updated_at = utcnow()
    db.commit()
    return activity_out(activity)


@api_router.post("/activities/{activity_id}/restore")
def restore_activity(activity_id: str, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activity = db.query(Activity).filter(Activity.id == activity_id, Activity.user_id == user_id).first()
    if activity is None:
        raise HTTPException(status_code=404, detail="Activity not found")
    activity.archived = False
    activity.updated_at = utcnow()
    db.commit()
    return activity_out(activity)


# ---------------------------------------------------------------------------
# Time entries
# ---------------------------------------------------------------------------

class TimeEntryIn(BaseModel):
    id: Optional[str] = None
    activity_id: str
    parent_activity_ids: str = ""
    started_at: str
    ended_at: Optional[str] = None
    duration_seconds: int = 0
    comment: str = ""
    tags: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    deleted_at: Optional[str] = None


def entry_out(e: TimeEntry) -> dict:
    return {
        "id": e.id,
        "activity_id": e.activity_id,
        "parent_activity_ids": e.parent_activity_ids,
        "started_at": iso(e.started_at),
        "ended_at": iso(e.ended_at),
        "duration_seconds": e.duration_seconds,
        "comment": e.comment,
        "tags": e.tags,
        "created_at": iso(e.created_at),
        "updated_at": iso(e.updated_at),
        "sync_status": e.sync_status,
        "deleted_at": iso(e.deleted_at),
    }


@api_router.get("/time-entries")
def list_time_entries(
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_id: str = Depends(require_user),
):
    query = db.query(TimeEntry).filter(TimeEntry.user_id == user_id, TimeEntry.deleted_at.is_(None))
    if from_:
        query = query.filter(TimeEntry.started_at >= parse_dt(from_))
    if to:
        query = query.filter(TimeEntry.started_at < parse_dt(to))
    return [entry_out(e) for e in query.order_by(TimeEntry.started_at).all()]


@api_router.post("/time-entries")
def create_time_entry(body: TimeEntryIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    ended = parse_dt(body.ended_at) if body.ended_at else None
    if body.id:
        existing = db.query(TimeEntry).filter(TimeEntry.id == body.id, TimeEntry.user_id == user_id).first()
        if existing is not None:
            if existing.deleted_at is not None:
                # Restore previously deleted entry (e.g. Undo action)
                existing.deleted_at = None
                existing.activity_id = body.activity_id
                existing.parent_activity_ids = body.parent_activity_ids or ""
                existing.started_at = parse_dt(body.started_at)
                existing.ended_at = ended
                existing.duration_seconds = body.duration_seconds or 0
                existing.comment = body.comment
                existing.tags = body.tags or ""
                existing.updated_at = utcnow()
                existing.sync_status = "synced"
                db.commit()
                return entry_out(existing)
            else:
                raise HTTPException(status_code=409, detail="Time entry already exists")

    entry = TimeEntry(
        id=body.id or new_id(),
        user_id=user_id,
        activity_id=body.activity_id,
        parent_activity_ids=body.parent_activity_ids or "",
        started_at=parse_dt(body.started_at),
        ended_at=ended,
        duration_seconds=body.duration_seconds or 0,
        comment=body.comment,
        tags=body.tags or "",
        created_at=parse_dt(body.created_at) if body.created_at else utcnow(),
        updated_at=parse_dt(body.updated_at) if body.updated_at else utcnow(),
        sync_status="synced",
    )
    db.add(entry)
    db.commit()
    return entry_out(entry)


@api_router.patch("/time-entries/{entry_id}")
def update_time_entry(entry_id: str, body: TimeEntryIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    entry = db.query(TimeEntry).filter(TimeEntry.id == entry_id, TimeEntry.user_id == user_id).first()
    if entry is None:
        raise HTTPException(status_code=404, detail="Time entry not found")
    incoming_updated = parse_dt(body.updated_at) if body.updated_at else utcnow()
    if incoming_updated < _aware(entry.updated_at):
        db.add(
            ConflictLog(
                id=new_id(),
                user_id=user_id,
                entity_type="time_entry",
                entity_id=entry_id,
                resolution="server_kept_newer",
                detail=f"server_updated_at={iso(entry.updated_at)} incoming_updated_at={body.updated_at}",
            ),
        )
        db.commit()
        return entry_out(entry)
    entry.activity_id = body.activity_id
    entry.parent_activity_ids = body.parent_activity_ids
    entry.started_at = parse_dt(body.started_at)
    entry.ended_at = parse_dt(body.ended_at) if body.ended_at else None
    entry.duration_seconds = body.duration_seconds
    entry.comment = body.comment
    entry.tags = body.tags
    entry.updated_at = incoming_updated
    entry.sync_status = "synced"
    db.commit()
    return entry_out(entry)


@api_router.delete("/time-entries/{entry_id}")
def delete_time_entry(entry_id: str, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    entry = db.query(TimeEntry).filter(TimeEntry.id == entry_id, TimeEntry.user_id == user_id).first()
    if entry is None:
        raise HTTPException(status_code=404, detail="Time entry not found")
    entry.deleted_at = utcnow()
    entry.updated_at = utcnow()
    entry.sync_status = "synced"
    db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Timer (Start / Stop / Current)
# ---------------------------------------------------------------------------

class TimerStartIn(BaseModel):
    activity_id: Optional[str] = None
    activity_name: Optional[str] = None
    comment: Optional[str] = ""
    tags: Optional[str] = ""
    started_at: Optional[str] = None


class TimerStopIn(BaseModel):
    entry_id: Optional[str] = None
    ended_at: Optional[str] = None


class AssistIn(BaseModel):
    text: str


class AssistOut(BaseModel):
    intent: str  # "start", "stop", "status", "unknown"
    response: str
    running: bool
    activity_name: Optional[str] = None
    duration_seconds: Optional[int] = None
    entry: Optional[dict] = None


@api_router.get("/timer/current")
def get_current_timer(db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    running = (
        db.query(TimeEntry)
        .filter(
            TimeEntry.user_id == user_id,
            TimeEntry.ended_at.is_(None),
            TimeEntry.deleted_at.is_(None),
        )
        .order_by(TimeEntry.started_at.desc())
        .first()
    )
    if not running:
        return {"running": False, "entry": None}
    activity = db.query(Activity).filter(Activity.id == running.activity_id, Activity.user_id == user_id).first()
    data = entry_out(running)
    data["activity"] = activity_out(activity) if activity else None
    now = utcnow()
    started = _aware(running.started_at)
    data["duration_seconds"] = max(0, int((now - started).total_seconds()))
    return {"running": True, "entry": data}


def get_tracking_status(db: Session, user_id: str) -> dict:
    """Helper to return standardized status dictionary for Home Assistant and WebSockets."""
    curr = get_current_timer(db=db, user_id=user_id)
    if curr.get("running") and curr.get("entry"):
        entry = curr["entry"]
        act = entry.get("activity")
        act_name = act.get("name") if act else "Aktiv"
        dur_sec = entry.get("duration_seconds", 0)
        return {
            "running": True,
            "state": "tracking",
            "activity": act_name,
            "duration_seconds": dur_sec,
            "duration_minutes": max(0, dur_sec // 60),
            "started_at": entry.get("started_at"),
            "entry_id": entry.get("id"),
        }
    return {
        "running": False,
        "state": "idle",
        "activity": None,
        "duration_seconds": 0,
        "duration_minutes": 0,
        "started_at": None,
        "entry_id": None,
    }


@api_router.post("/timer/start")
def start_timer(body: TimerStartIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activity = None
    if body.activity_id:
        activity = db.query(Activity).filter(Activity.id == body.activity_id, Activity.user_id == user_id).first()
    elif body.activity_name:
        name_clean = body.activity_name.strip()
        # 1. Exact match (case-insensitive)
        activity = (
            db.query(Activity)
            .filter(
                Activity.user_id == user_id,
                Activity.deleted_at.is_(None),
                func.lower(Activity.name) == name_clean.lower(),
            )
            .first()
        )
        if not activity:
            all_acts = (
                db.query(Activity)
                .filter(Activity.user_id == user_id, Activity.deleted_at.is_(None), Activity.archived.is_(False))
                .all()
            )
            # 2. Substring or prefix match
            activity = next(
                (a for a in all_acts if name_clean.lower() in a.name.lower() or a.name.lower() in name_clean.lower()),
                None,
            )
            # 3. Fuzzy match
            if not activity:
                matches = difflib.get_close_matches(name_clean.lower(), [a.name.lower() for a in all_acts], n=1, cutoff=0.5)
                if matches:
                    activity = next((a for a in all_acts if a.name.lower() == matches[0]), None)
    else:
        raise HTTPException(status_code=400, detail="Either activity_id or activity_name must be provided")

    if activity is None:
        raise HTTPException(status_code=404, detail="Activity not found")

    now = utcnow()
    start_dt = parse_dt(body.started_at) if body.started_at else now

    # Stop any running timers for this user first
    active_entries = (
        db.query(TimeEntry)
        .filter(
            TimeEntry.user_id == user_id,
            TimeEntry.ended_at.is_(None),
            TimeEntry.deleted_at.is_(None),
        )
        .all()
    )
    for active in active_entries:
        active.ended_at = start_dt
        active.duration_seconds = max(0, int((_aware(start_dt) - _aware(active.started_at)).total_seconds()))
        active.updated_at = now
        active.sync_status = "synced"

    entry = TimeEntry(
        id=new_id(),
        user_id=user_id,
        activity_id=activity.id,
        parent_activity_ids=activity.parent_activity_id or "",
        started_at=start_dt,
        ended_at=None,
        duration_seconds=0,
        comment=body.comment or "",
        tags=body.tags or "",
        created_at=now,
        updated_at=now,
        sync_status="synced",
    )
    db.add(entry)
    db.commit()

    data = entry_out(entry)
    data["activity"] = activity_out(activity)
    data["duration_seconds"] = max(0, int((now - _aware(entry.started_at)).total_seconds()))

    st = get_tracking_status(db=db, user_id=user_id)
    manager.broadcast_sync(user_id, {"event": "timer_started", "data": st})
    publish_discovery_and_state(
        running=True,
        activity_name=activity.name,
        duration_seconds=data["duration_seconds"],
        started_at=iso(entry.started_at),
    )
    return {"running": True, "entry": data}


@api_router.post("/timer/stop")
def stop_timer(body: Optional[TimerStopIn] = Body(default=None), db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    now = utcnow()
    entry_id = body.entry_id if body else None
    stop_dt = parse_dt(body.ended_at) if (body and body.ended_at) else now

    query = db.query(TimeEntry).filter(
        TimeEntry.user_id == user_id,
        TimeEntry.ended_at.is_(None),
        TimeEntry.deleted_at.is_(None),
    )
    if entry_id:
        query = query.filter(TimeEntry.id == entry_id)

    running_entries = query.order_by(TimeEntry.started_at.desc()).all()
    if not running_entries:
        return {"running": False, "stopped": None}

    stopped_entries = []
    for entry in running_entries:
        entry.ended_at = stop_dt
        entry.duration_seconds = max(0, int((_aware(stop_dt) - _aware(entry.started_at)).total_seconds()))
        entry.updated_at = now
        entry.sync_status = "synced"
        data = entry_out(entry)
        act = db.query(Activity).filter(Activity.id == entry.activity_id).first()
        if act:
            data["activity_name"] = act.name
            data["activity"] = activity_out(act)
        stopped_entries.append(data)

    db.commit()

    st = get_tracking_status(db=db, user_id=user_id)
    manager.broadcast_sync(user_id, {"event": "timer_stopped", "data": st})
    publish_discovery_and_state(
        running=False,
        activity_name=None,
        duration_seconds=0,
        started_at=None,
    )
    return {"running": False, "stopped": stopped_entries[0] if stopped_entries else None}


# ---------------------------------------------------------------------------
# Home Assistant Assist / Voice Control & Sensor
# ---------------------------------------------------------------------------

@api_router.get("/assist/status")
def assist_status(db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    """Convenient endpoint for Home Assistant REST sensors."""
    return get_tracking_status(db=db, user_id=user_id)


@api_router.post("/assist/process", response_model=AssistOut)
def assist_process(body: AssistIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    """Processes natural language voice/assist commands with fuzzy activity matching."""
    raw = body.text.strip()
    low = raw.lower()

    # 1. Stop Intent
    stop_triggers = ["stopp", "stop", "beende", "anhalt", "fertig", "aus", "pause beenden", "aufhören", "halt an"]
    if any(t in low for t in stop_triggers) and not any(t in low for t in ["was ", "status", "wie lange"]):
        stop_res = stop_timer(body=None, db=db, user_id=user_id)
        stopped = stop_res.get("stopped")
        if stopped:
            act_name = stopped.get("activity_name") or "Aktivität"
            dur_sec = stopped.get("duration_seconds") or 0
            dur_min = max(1, dur_sec // 60)
            return AssistOut(
                intent="stop",
                response=f"Tracking für {act_name} nach {dur_min} Minuten beendet.",
                running=False,
                activity_name=act_name,
                duration_seconds=dur_sec,
                entry=stopped,
            )
        return AssistOut(
            intent="stop",
            response="Aktuell läuft keine aktive Zeiterfassung zum Stoppen.",
            running=False,
        )

    # 2. Status / Query Intent
    query_triggers = ["was läuft", "was tracke", "status", "wie lange", "läuft gerade", "was mache ich", "aktuelle aktivität"]
    if any(t in low for t in query_triggers) or low in ["was", "läuft was", "läuft noch was"]:
        curr = get_current_timer(db=db, user_id=user_id)
        if curr.get("running") and curr.get("entry"):
            entry = curr["entry"]
            act = entry.get("activity")
            act_name = act.get("name") if act else "Unbekannt"
            dur_sec = entry.get("duration_seconds", 0)
            dur_min = max(0, dur_sec // 60)
            return AssistOut(
                intent="status",
                response=f"Aktuell läuft seit {dur_min} Minuten die Aktivität {act_name}.",
                running=True,
                activity_name=act_name,
                duration_seconds=dur_sec,
                entry=entry,
            )
        return AssistOut(
            intent="status",
            response="Aktuell läuft keine Zeiterfassung.",
            running=False,
        )

    # 3. Start Intent: Extract candidate activity text
    candidate = re.sub(
        r"^(bitte\s+)?(starte|tracke|beginne|logge|erfasse|mache|nimm|start|track)\s+(mal\s+)?(das\s+|die\s+|den\s+|ein\s+|eine\s+)?(tracking\s+(von|für)\s+|zeiterfassung\s+(von|für)\s+)?",
        "",
        low,
        flags=re.IGNORECASE,
    ).strip()
    candidate = re.sub(r"^(die|das|der|den|ein|eine|für|von)\s+", "", candidate).strip()
    candidate = candidate.rstrip(".!?,")

    if not candidate:
        return AssistOut(
            intent="unknown",
            response="Welche Aktivität möchtest du starten?",
            running=False,
        )

    all_acts = (
        db.query(Activity)
        .filter(Activity.user_id == user_id, Activity.deleted_at.is_(None), Activity.archived.is_(False))
        .all()
    )
    if not all_acts:
        return AssistOut(
            intent="unknown",
            response="Es sind noch keine Aktivitäten im TimeTracker angelegt.",
            running=False,
        )

    # A) Exact or case-insensitive match
    matched = next((a for a in all_acts if a.name.lower() == candidate), None)

    # B) Substring / prefix match
    if not matched:
        matched = next((a for a in all_acts if candidate in a.name.lower() or a.name.lower() in candidate), None)

    # C) Fuzzy match using difflib
    if not matched:
        matches = difflib.get_close_matches(candidate, [a.name.lower() for a in all_acts], n=1, cutoff=0.45)
        if matches:
            matched = next((a for a in all_acts if a.name.lower() == matches[0]), None)

    if matched:
        start_res = start_timer(body=TimerStartIn(activity_id=matched.id), db=db, user_id=user_id)
        return AssistOut(
            intent="start",
            response=f"Tracking für {matched.name} gestartet.",
            running=True,
            activity_name=matched.name,
            entry=start_res.get("entry"),
        )
    else:
        top_names = ", ".join(a.name for a in all_acts[:4])
        return AssistOut(
            intent="unknown",
            response=f"Aktivität '{candidate}' wurde nicht gefunden. Verfügbar sind z. B.: {top_names}.",
            running=False,
        )


@api_router.get("/assist/mqtt_discovery")
def assist_mqtt_discovery(user_id: str = Depends(require_user)):
    """Returns Home Assistant MQTT Autodiscovery configurations and state topics."""
    return {
        "device": get_ha_device_info(),
        "configs": get_ha_discovery_payloads(),
        "state_topic": f"{MQTT_TOPIC_PREFIX}/state",
    }


@api_router.websocket("/ws")
async def websocket_events(websocket: WebSocket, token: Optional[str] = Query(None)):
    """Real-time event stream over WebSocket for Web dashboards and Home Assistant."""
    db = SessionLocal()
    user_id = None
    try:
        if token:
            user_id = authenticate_token(token, db)
        if not user_id:
            await websocket.close(code=4003)
            return

        await manager.connect(user_id, websocket)

        # Immediately send current state snapshot
        status = assist_status(db=db, user_id=user_id)
        await websocket.send_json({
            "event": "connected",
            "server_time": iso(utcnow()),
            "status": status,
        })

        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)
                action = data.get("action")
                if action == "status":
                    st = assist_status(db=db, user_id=user_id)
                    await websocket.send_json({"event": "status", "data": st})
                elif action == "ping":
                    await websocket.send_json({"event": "pong", "time": iso(utcnow())})
            except Exception:
                pass
    except WebSocketDisconnect:
        if user_id:
            await manager.disconnect(user_id, websocket)
    except Exception:
        if user_id:
            await manager.disconnect(user_id, websocket)
    finally:
        db.close()



# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def _day_bounds(date_str: str) -> tuple:
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return day, day + timedelta(days=1)


@api_router.get("/dashboard/day")
def dashboard_day(date: str, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    start, end = _day_bounds(date)
    entries = (
        db.query(TimeEntry)
        .filter(
            TimeEntry.user_id == user_id,
            TimeEntry.deleted_at.is_(None),
            TimeEntry.started_at >= start,
            TimeEntry.started_at < end,
        )
        .order_by(TimeEntry.started_at)
        .all()
    )
    total = sum((e.duration_seconds or 0) for e in entries)
    return {
        "date": date,
        "total_seconds": total,
        "entries": [entry_out(e) for e in entries],
    }


@api_router.get("/dashboard/summary")
def dashboard_summary(
    from_: str = Query(alias="from"),
    to: str = Query(alias="to"),
    group_by: str = "activity",
    db: Session = Depends(get_db),
    user_id: str = Depends(require_user),
):
    start = parse_dt(from_)
    end = parse_dt(to)
    entries = (
        db.query(TimeEntry)
        .filter(
            TimeEntry.user_id == user_id,
            TimeEntry.deleted_at.is_(None),
            TimeEntry.started_at >= start,
            TimeEntry.started_at < end,
        )
        .all()
    )
    total = sum((e.duration_seconds or 0) for e in entries)
    buckets: dict = {}
    for e in entries:
        key = e.activity_id
        current = buckets.get(key, 0)
        buckets[key] = current + (e.duration_seconds or 0)
    return {
        "from": from_,
        "to": to,
        "total_seconds": total,
        "by_activity": [{"activity_id": k, "seconds": v} for k, v in sorted(buckets.items())],
    }


@api_router.get("/dashboard/heatmap")
def dashboard_heatmap(
    from_: str = Query(alias="from"),
    to: str = Query(alias="to"),
    db: Session = Depends(get_db),
    user_id: str = Depends(require_user),
):
    start = parse_dt(from_)
    end = parse_dt(to)
    rows = (
        db.query(
            func.date(TimeEntry.started_at).label("day"),
            func.sum(TimeEntry.duration_seconds).label("seconds"),
        )
        .filter(
            TimeEntry.user_id == user_id,
            TimeEntry.deleted_at.is_(None),
            TimeEntry.started_at >= start,
            TimeEntry.started_at < end,
        )
        .group_by("day")
        .all()
    )
    return {"days": [{"date": str(r.day), "seconds": int(r.seconds or 0)} for r in rows]}


# ---------------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------------

class GoalIn(BaseModel):
    id: Optional[str] = None
    activity_id: str
    goal_type: str = "seconds_per_week"
    goal_value: float = 0
    updated_at: Optional[str] = None


def goal_out(g: Goal) -> dict:
    return {
        "id": g.id,
        "activity_id": g.activity_id,
        "goal_type": g.goal_type,
        "goal_value": g.goal_value,
        "created_at": iso(g.created_at),
        "updated_at": iso(g.updated_at),
        "deleted_at": iso(g.deleted_at),
    }


@api_router.get("/goals")
def list_goals(db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    goals = db.query(Goal).filter(Goal.user_id == user_id, Goal.deleted_at.is_(None)).all()
    return [goal_out(g) for g in goals]


@api_router.post("/goals")
def create_goal(body: GoalIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    goal = Goal(
        id=body.id or new_id(),
        user_id=user_id,
        activity_id=body.activity_id,
        goal_type=body.goal_type,
        goal_value=body.goal_value,
        updated_at=parse_dt(body.updated_at) if body.updated_at else utcnow(),
    )
    db.add(goal)
    db.commit()
    return goal_out(goal)


@api_router.patch("/goals/{goal_id}")
def update_goal(goal_id: str, body: GoalIn, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    goal = db.query(Goal).filter(Goal.id == goal_id, Goal.user_id == user_id).first()
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    goal.activity_id = body.activity_id
    goal.goal_type = body.goal_type
    goal.goal_value = body.goal_value
    goal.updated_at = parse_dt(body.updated_at) if body.updated_at else utcnow()
    db.commit()
    return goal_out(goal)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class SyncPushItem(BaseModel):
    entity_type: str
    data: dict


class SyncPushRequest(BaseModel):
    items: list[SyncPushItem]


TIMETABLE_TYPES = (
    "timetable_event",
    "timetable_override",
    "timetable_day",
    "timetable_todo",
    "subject_goal",
)


def is_resurrection(existing, incoming_deleted) -> bool:
    """A non tombstone push can not bring a deleted entity back.

    Deletions always win: a device that has not seen the tombstone yet
    would otherwise resurrect the entity with its next stale push.
    Re-adding an entity creates a new id, so nothing is lost.
    """
    return (
        existing is not None
        and existing.deleted_at is not None
        and incoming_deleted is None
    )


@api_router.post("/sync/push")
def sync_push(body: SyncPushRequest, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    applied = 0
    conflicts = []

    # Batch prefetch existing entities to eliminate N+1 queries
    activity_ids = [item.data["id"] for item in body.items if item.entity_type == "activity" and "id" in item.data]
    entry_ids = [item.data["id"] for item in body.items if item.entity_type == "time_entry" and "id" in item.data]
    category_ids = [item.data["id"] for item in body.items if item.entity_type == "category" and "id" in item.data]
    tag_ids = [item.data["id"] for item in body.items if item.entity_type == "record_tag" and "id" in item.data]
    timetable_ids = [item.data["id"] for item in body.items if item.entity_type in TIMETABLE_TYPES and "id" in item.data]

    activities_map = {a.id: a for a in db.query(Activity).filter(Activity.id.in_(activity_ids)).all()} if activity_ids else {}
    entries_map = {e.id: e for e in db.query(TimeEntry).filter(TimeEntry.id.in_(entry_ids)).all()} if entry_ids else {}
    categories_map = {c.id: c for c in db.query(SyncCategory).filter(SyncCategory.id.in_(category_ids)).all()} if category_ids else {}
    tags_map = {t.id: t for t in db.query(SyncTag).filter(SyncTag.id.in_(tag_ids)).all()} if tag_ids else {}
    timetable_map = {(t.id, t.entity_type): t for t in db.query(SyncTimetable).filter(SyncTimetable.id.in_(timetable_ids)).all()} if timetable_ids else {}

    for item in body.items:
        data = item.data
        entity_type = item.entity_type
        incoming_updated = parse_dt(data.get("updated_at")) if data.get("updated_at") else utcnow()
        incoming_deleted = parse_dt(data["deleted_at"]) if data.get("deleted_at") else None
        if entity_type == "activity":
            existing = activities_map.get(data["id"])
            if existing is not None and existing.user_id != user_id:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "rejected"})
                continue
            if existing is None:
                if incoming_deleted is not None:
                    # Tombstone for an unknown activity: nothing to delete.
                    applied += 1
                    continue
                new_act = Activity(
                    id=data["id"],
                    user_id=user_id,
                    name=data.get("name", ""),
                    color=data.get("color", ""),
                    color_id=data.get("color_id", 0),
                    icon=data.get("icon", ""),
                    sort_order=data.get("sort_order", 0),
                    archived=data.get("archived", False),
                    parent_activity_id=data.get("parent_activity_id"),
                    category=data.get("category", ""),
                    goal_seconds_per_week=data.get("goal_seconds_per_week"),
                    goal_seconds_total=data.get("goal_seconds_total"),
                    goal_days_per_month=data.get("goal_days_per_month"),
                    created_at=parse_dt(data["created_at"]) if data.get("created_at") else utcnow(),
                    updated_at=incoming_updated,
                )
                db.add(new_act)
                activities_map[data["id"]] = new_act
                applied += 1
            elif is_resurrection(existing, incoming_deleted):
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "tombstone_kept"})
            elif incoming_updated >= _aware(existing.updated_at):
                existing.name = data.get("name", existing.name)
                existing.color = data.get("color", existing.color)
                existing.color_id = data.get("color_id", existing.color_id)
                existing.icon = data.get("icon", existing.icon)
                existing.sort_order = data.get("sort_order", existing.sort_order)
                existing.archived = data.get("archived", existing.archived)
                existing.parent_activity_id = data.get("parent_activity_id")
                existing.category = data.get("category", existing.category)
                existing.goal_seconds_per_week = data.get("goal_seconds_per_week")
                existing.goal_seconds_total = data.get("goal_seconds_total")
                existing.goal_days_per_month = data.get("goal_days_per_month")
                existing.deleted_at = incoming_deleted
                existing.updated_at = incoming_updated
                applied += 1
            else:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
        elif entity_type == "time_entry":
            existing = entries_map.get(data["id"])
            if existing is not None and existing.user_id != user_id:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "rejected"})
                continue
            if existing is None:
                if incoming_deleted is not None:
                    # Tombstone for an unknown entry: nothing to delete.
                    applied += 1
                    continue
                if not data.get("activity_id") or not data.get("started_at"):
                    conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "invalid"})
                    continue
                new_entry = TimeEntry(
                    id=data["id"],
                    user_id=user_id,
                    activity_id=data["activity_id"],
                    parent_activity_ids=data.get("parent_activity_ids", ""),
                    started_at=parse_dt(data["started_at"]),
                    ended_at=parse_dt(data["ended_at"]) if data.get("ended_at") else None,
                    duration_seconds=data.get("duration_seconds", 0),
                    comment=data.get("comment", ""),
                    tags=data.get("tags", ""),
                    created_at=parse_dt(data["created_at"]) if data.get("created_at") else utcnow(),
                    updated_at=incoming_updated,
                    sync_status="synced",
                )
                db.add(new_entry)
                entries_map[data["id"]] = new_entry
                applied += 1
            elif incoming_deleted is not None:
                # Tombstone for a known entry: mark deleted, keep content.
                if incoming_updated >= _aware(existing.updated_at):
                    existing.deleted_at = incoming_deleted
                    existing.updated_at = incoming_updated
                    existing.sync_status = "synced"
                    applied += 1
                else:
                    conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
            elif is_resurrection(existing, incoming_deleted):
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "tombstone_kept"})
            elif incoming_updated >= _aware(existing.updated_at):
                existing.activity_id = data["activity_id"]
                existing.parent_activity_ids = data.get("parent_activity_ids", "")
                existing.started_at = parse_dt(data["started_at"])
                existing.ended_at = parse_dt(data["ended_at"]) if data.get("ended_at") else None
                existing.duration_seconds = data.get("duration_seconds", 0)
                existing.comment = data.get("comment", "")
                existing.tags = data.get("tags", "")
                existing.deleted_at = incoming_deleted
                existing.updated_at = incoming_updated
                existing.sync_status = "synced"
                applied += 1
            else:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
        elif entity_type in ("category", "record_tag"):
            # Generic opaque payload storage: the app owns the data layout.
            table = SyncCategory if entity_type == "category" else SyncTag
            current_map = categories_map if entity_type == "category" else tags_map
            existing = current_map.get(data["id"])
            if existing is not None and existing.user_id != user_id:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "rejected"})
                continue
            incoming_deleted = parse_dt(data["deleted_at"]) if data.get("deleted_at") else None
            if existing is None:
                if incoming_deleted is not None:
                    # Tombstone for an unknown entity: nothing to delete.
                    applied += 1
                    continue
                new_cat_or_tag = table(
                    id=data["id"],
                    user_id=user_id,
                    data=json.dumps(data),
                    updated_at=incoming_updated,
                )
                db.add(new_cat_or_tag)
                current_map[data["id"]] = new_cat_or_tag
                applied += 1
            elif incoming_deleted is not None:
                if incoming_updated >= _aware(existing.updated_at):
                    existing.deleted_at = incoming_deleted
                    existing.updated_at = incoming_updated
                    applied += 1
                else:
                    conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
            elif is_resurrection(existing, incoming_deleted):
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "tombstone_kept"})
            elif incoming_updated >= _aware(existing.updated_at):
                existing.data = json.dumps(data)
                existing.deleted_at = None
                existing.updated_at = incoming_updated
                applied += 1
            else:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
        elif entity_type in TIMETABLE_TYPES:
            # Generic opaque payload storage: the app owns the data layout.
            existing = timetable_map.get((data["id"], entity_type))
            if existing is not None and existing.user_id != user_id:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "rejected"})
                continue
            if existing is None:
                if incoming_deleted is not None:
                    # Tombstone for an unknown entity: nothing to delete.
                    applied += 1
                    continue
                new_tt = SyncTimetable(
                    id=data["id"],
                    user_id=user_id,
                    entity_type=entity_type,
                    data=json.dumps(data),
                    updated_at=incoming_updated,
                )
                db.add(new_tt)
                timetable_map[(data["id"], entity_type)] = new_tt
                applied += 1
            elif is_resurrection(existing, incoming_deleted):
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "tombstone_kept"})
            elif incoming_deleted is not None:
                if incoming_updated >= _aware(existing.updated_at):
                    existing.deleted_at = incoming_deleted
                    existing.updated_at = incoming_updated
                    applied += 1
                else:
                    conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
            elif incoming_updated >= _aware(existing.updated_at):
                existing.data = json.dumps(data)
                existing.deleted_at = None
                existing.updated_at = incoming_updated
                applied += 1
            else:
                conflicts.append({"entity_type": entity_type, "id": data["id"], "resolution": "server_kept_newer"})
        else:
            conflicts.append({"entity_type": entity_type, "id": data.get("id", ""), "resolution": "unknown_type"})
    for conflict in conflicts:
        db.add(
            ConflictLog(
                id=new_id(),
                user_id=user_id,
                entity_type=conflict["entity_type"],
                entity_id=conflict["id"],
                resolution=conflict["resolution"],
            ),
        )
    db.commit()
    manager.broadcast_sync(user_id, {"event": "sync_completed", "data": {"applied": applied, "conflicts": len(conflicts)}})
    return {"applied": applied, "conflicts": conflicts, "server_time": iso(utcnow())}


@api_router.get("/sync/pull")
def sync_pull(
    since: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_id: str = Depends(require_user),
):
    since_dt = parse_dt(since) if since else datetime(1970, 1, 1, tzinfo=timezone.utc)
    activities = db.query(Activity).filter(Activity.user_id == user_id, Activity.updated_at > since_dt).all()
    entries = db.query(TimeEntry).filter(TimeEntry.user_id == user_id, TimeEntry.updated_at > since_dt).all()
    goals = db.query(Goal).filter(Goal.user_id == user_id, Goal.updated_at > since_dt).all()
    categories = db.query(SyncCategory).filter(SyncCategory.user_id == user_id, SyncCategory.updated_at > since_dt).all()
    tags = db.query(SyncTag).filter(SyncTag.user_id == user_id, SyncTag.updated_at > since_dt).all()

    def generic_out(row) -> dict:
        return {
            **json.loads(row.data),
            "updated_at": iso(row.updated_at),
            "deleted_at": iso(row.deleted_at),
        }

    timetable_rows = (
        db.query(SyncTimetable)
        .filter(SyncTimetable.user_id == user_id, SyncTimetable.updated_at > since_dt)
        .all()
    )
    timetable_out: dict[str, list[dict]] = {
        kind: [] for kind in (
            "timetable_events",
            "timetable_overrides",
            "timetable_days",
            "timetable_todos",
            "subject_goals",
        )
    }
    for row in timetable_rows:
        kind = row.entity_type + "s"
        if kind in timetable_out:
            timetable_out[kind].append(generic_out(row))

    return {
        **{kind: items for kind, items in timetable_out.items()},
        "activities": [activity_out(a) for a in activities],
        "time_entries": [entry_out(e) for e in entries],
        "goals": [goal_out(g) for g in goals],
        "categories": [generic_out(c) for c in categories],
        "tags": [generic_out(t) for t in tags],
        "server_time": iso(utcnow()),
    }


@api_router.get("/sync/conflicts")
def sync_conflicts(db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    rows = (
        db.query(ConflictLog)
        .filter(ConflictLog.user_id == user_id)
        .order_by(ConflictLog.created_at.desc())
        .limit(100)
        .all()
    )
    return [
        {
            "id": r.id,
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "resolution": r.resolution,
            "detail": r.detail,
            "created_at": iso(r.created_at),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Export / Import
# ---------------------------------------------------------------------------

@api_router.get("/export")
def export_data(format: str = "json", db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    activities = db.query(Activity).filter(Activity.user_id == user_id).all()
    entries = db.query(TimeEntry).filter(TimeEntry.user_id == user_id).all()
    goals = db.query(Goal).filter(Goal.user_id == user_id).all()
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "activity_id", "started_at", "ended_at", "duration_seconds", "comment", "tags"])
        for e in entries:
            writer.writerow([e.id, e.activity_id, iso(e.started_at), iso(e.ended_at), e.duration_seconds, e.comment, e.tags])
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=timetracker.csv"},
        )
    data = {
        "activities": [activity_out(a) for a in activities],
        "time_entries": [entry_out(e) for e in entries],
        "goals": [goal_out(g) for g in goals],
        "exported_at": iso(utcnow()),
    }
    return Response(content=json.dumps(data, indent=2), media_type="application/json")


class SimpleTimeTrackerCsvRow(BaseModel):
    rows: list[dict]


@api_router.post("/import/simple-time-tracker")
def import_simple_time_tracker(body: SimpleTimeTrackerCsvRow, db: Session = Depends(get_db), user_id: str = Depends(require_user)):
    """Import records from the Simple Time Tracker CSV export format.

    Expected columns: activity name, start time, end time (or duration), optional comment.
    """
    created = 0
    for row in body.rows:
        name = row.get("activity") or row.get("name") or ""
        if not name:
            continue
        activity = db.query(Activity).filter(Activity.user_id == user_id, Activity.name == name).first()
        if activity is None:
            activity = Activity(id=new_id(), user_id=user_id, name=name)
            db.add(activity)
            db.flush()
        started = row.get("started_at") or row.get("start")
        ended = row.get("ended_at") or row.get("end")
        if not started:
            continue
        start_dt = parse_dt(started)
        end_dt = parse_dt(ended) if ended else None
        duration = row.get("duration_seconds")
        if duration is None and end_dt is not None:
            duration = int((end_dt - start_dt).total_seconds())
        db.add(
            TimeEntry(
                id=new_id(),
                user_id=user_id,
                activity_id=activity.id,
                started_at=start_dt,
                ended_at=end_dt,
                duration_seconds=int(duration or 0),
                comment=row.get("comment", ""),
            ),
        )
        created += 1
    db.commit()
    return {"created": created}
