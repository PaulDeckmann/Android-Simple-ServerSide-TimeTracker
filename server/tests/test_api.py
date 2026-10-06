"""Smoke tests for the time tracker server API.

Uses an in-memory SQLite database so the tests run anywhere
(the production deployment uses PostgreSQL via docker-compose).
"""
import os
import tempfile

os.environ["DATABASE_URL"] = "sqlite://"
os.environ["SECRET_KEY_FILE"] = os.path.join(tempfile.mkdtemp(), "secret.key")

from fastapi.testclient import TestClient  # noqa: E402

from app.db import Base, engine, SessionLocal, User, new_id  # noqa: E402
from app.main import app  # noqa: E402
from app.security import hash_password  # noqa: E402


def seed_user() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    db.add(User(id=new_id(), username="test", password_hash=hash_password("password123")))
    db.commit()
    db.close()


_cached_token = None

def login(client: TestClient) -> str:
    global _cached_token
    if _cached_token is not None:
        return _cached_token
    response = client.post("/api/auth/login", json={"username": "test", "password": "password123"})
    assert response.status_code == 200, response.text
    _cached_token = response.json()["access_token"]
    return _cached_token



def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_full_flow():
    seed_user()
    client = TestClient(app)

    token = login(client)

    # Activity lifecycle.
    response = client.post(
        "/api/activities",
        json={"name": "Thermodynamik", "color": "3", "icon": "ic_school_24px"},
        headers=auth_headers(token),
    )
    assert response.status_code == 200, response.text
    activity = response.json()

    response = client.get("/api/activities", headers=auth_headers(token))
    assert response.status_code == 200
    assert len(response.json()) == 1

    # Archive hides activity from the default list.
    response = client.post(f"/api/activities/{activity['id']}/archive", headers=auth_headers(token))
    assert response.status_code == 200
    response = client.get("/api/activities", headers=auth_headers(token))
    assert len(response.json()) == 0
    response = client.get("/api/activities?include_archived=true", headers=auth_headers(token))
    assert len(response.json()) == 1

    # Restore.
    response = client.post(f"/api/activities/{activity['id']}/restore", headers=auth_headers(token))
    assert response.status_code == 200

    # Time entry.
    entry_payload = {
        "activity_id": activity["id"],
        "started_at": "2026-01-01T08:00:00+00:00",
        "ended_at": "2026-01-01T09:00:00+00:00",
        "duration_seconds": 3600,
    }
    response = client.post("/api/time-entries", json=entry_payload, headers=auth_headers(token))
    assert response.status_code == 200, response.text
    entry = response.json()

    response = client.get("/api/time-entries", headers=auth_headers(token))
    assert len(response.json()) == 1

    # Dashboard day summary.
    response = client.get("/api/dashboard/day?date=2026-01-01", headers=auth_headers(token))
    assert response.status_code == 200
    assert response.json()["total_seconds"] == 3600

    # Sync push with a newer version updates, older version conflicts.
    newer = dict(entry_payload)
    newer["id"] = entry["id"]
    newer["comment"] = "updated"
    newer["updated_at"] = "2099-01-02T00:00:00+00:00"
    response = client.post(
        "/api/sync/push",
        json={"items": [{"entity_type": "time_entry", "data": newer}]},
        headers=auth_headers(token),
    )
    assert response.status_code == 200
    assert response.json()["applied"] == 1

    older = dict(newer)
    older["comment"] = "stale"
    older["updated_at"] = "2000-01-01T00:00:00+00:00"
    response = client.post(
        "/api/sync/push",
        json={"items": [{"entity_type": "time_entry", "data": older}]},
        headers=auth_headers(token),
    )
    assert response.status_code == 200
    conflicts = response.json()["conflicts"]
    assert len(conflicts) == 1
    assert conflicts[0]["resolution"] == "server_kept_newer"

    # Conflict log is visible.
    response = client.get("/api/sync/conflicts", headers=auth_headers(token))
    assert response.status_code == 200
    assert len(response.json()) == 1

    # Sync pull returns everything.
    response = client.get("/api/sync/pull", headers=auth_headers(token))
    assert response.status_code == 200
    assert len(response.json()["time_entries"]) == 1

    # Soft delete keeps the row retrievable via pull.
    response = client.delete(f"/api/time-entries/{entry['id']}", headers=auth_headers(token))
    assert response.status_code == 200
    response = client.get("/api/time-entries", headers=auth_headers(token))
    assert len(response.json()) == 0
    response = client.get("/api/sync/pull", headers=auth_headers(token))
    pulled = response.json()["time_entries"]
    assert len(pulled) == 1
    assert pulled[0]["deleted_at"] is not None

    # Export json.
    response = client.get("/api/export?format=json", headers=auth_headers(token))
    assert response.status_code == 200
    assert "activities" in response.json()

    # Export csv.
    response = client.get("/api/export?format=csv", headers=auth_headers(token))
    assert response.status_code == 200
    assert "activity_id" in response.text

    # Import Simple Time Tracker CSV rows.
    response = client.post(
        "/api/import/simple-time-tracker",
        json={
            "rows": [
                {
                    "activity": "Lernen",
                    "started_at": "2026-01-01T10:00:00+00:00",
                    "ended_at": "2026-01-01T11:00:00+00:00",
                },
            ],
        },
        headers=auth_headers(token),
    )
    assert response.status_code == 200, response.text
    assert response.json()["created"] == 1


def test_auth_requires_token():
    client = TestClient(app)
    response = client.get("/api/activities")
    assert response.status_code == 401


def test_login_rate_limit():
    seed = os.environ.get("RATE_LIMIT_TEST", "0")
    if seed != "1":
        return
    client = TestClient(app)
    for _ in range(12):
        client.post("/api/auth/login", json={"username": "x", "password": "y"})
    response = client.post("/api/auth/login", json={"username": "x", "password": "y"})
    assert response.status_code == 429


def ensure_seed_user() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    if db.query(User).filter(User.username == "test").first() is None:
        db.add(User(id=new_id(), username="test", password_hash=hash_password("password123")))
        db.commit()
    db.close()


def push_item(client: TestClient, token: str, entity_type: str, data: dict) -> dict:
    response = client.post(
        "/api/sync/push",
        json={"items": [{"entity_type": entity_type, "data": data}]},
        headers=auth_headers(token),
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_sync_push_tombstone_deletes_known_entry():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    push_item(
        client,
        token,
        "activity",
        {"id": "a1", "name": "Lernen", "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    push_item(
        client,
        token,
        "time_entry",
        {
            "id": "e1",
            "activity_id": "a1",
            "started_at": "2026-01-01T08:00:00+00:00",
            "ended_at": "2026-01-01T09:00:00+00:00",
            "duration_seconds": 3600,
            "updated_at": "2026-01-01T00:00:00+00:00",
        },
    )

    result = push_item(
        client,
        token,
        "time_entry",
        {
            "id": "e1",
            "updated_at": "2026-01-02T00:00:00+00:00",
            "deleted_at": "2026-01-02T00:00:00+00:00",
        },
    )
    assert result["applied"] == 1
    assert result["conflicts"] == []

    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    entry = next(e for e in pulled["time_entries"] if e["id"] == "e1")
    assert entry["deleted_at"] is not None


def test_sync_push_tombstone_unknown_entities_is_noop():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    result = client.post(
        "/api/sync/push",
        json={
            "items": [
                {
                    "entity_type": "time_entry",
                    "data": {
                        "id": "e-unknown",
                        "updated_at": "2026-01-02T00:00:00+00:00",
                        "deleted_at": "2026-01-02T00:00:00+00:00",
                    },
                },
                {
                    "entity_type": "activity",
                    "data": {
                        "id": "a-unknown",
                        "updated_at": "2026-01-02T00:00:00+00:00",
                        "deleted_at": "2026-01-02T00:00:00+00:00",
                    },
                },
            ]
        },
        headers=auth_headers(token),
    )
    assert result.status_code == 200, result.text
    assert result.json()["applied"] == 2
    assert result.json()["conflicts"] == []


def test_sync_push_tombstone_deletes_known_activity():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    push_item(
        client,
        token,
        "activity",
        {"id": "a2", "name": "Vorlesung", "updated_at": "2026-01-01T00:00:00+00:00"},
    )

    result = push_item(
        client,
        token,
        "activity",
        {
            "id": "a2",
            "updated_at": "2026-01-02T00:00:00+00:00",
            "deleted_at": "2026-01-02T00:00:00+00:00",
        },
    )
    assert result["applied"] == 1

    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    activity = next(a for a in pulled["activities"] if a["id"] == "a2")
    assert activity["deleted_at"] is not None


def test_sync_push_tombstone_stale_entry_keeps_server_state():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    push_item(
        client,
        token,
        "activity",
        {"id": "a3", "name": "Tutorium", "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    push_item(
        client,
        token,
        "time_entry",
        {
            "id": "e3",
            "activity_id": "a3",
            "started_at": "2026-01-01T08:00:00+00:00",
            "ended_at": "2026-01-01T09:00:00+00:00",
            "updated_at": "2026-01-05T00:00:00+00:00",
        },
    )

    # A stale tombstone loses against the newer server version.
    result = push_item(
        client,
        token,
        "time_entry",
        {
            "id": "e3",
            "updated_at": "2026-01-02T00:00:00+00:00",
            "deleted_at": "2026-01-02T00:00:00+00:00",
        },
    )
    assert result["applied"] == 0
    assert result["conflicts"][0]["resolution"] == "server_kept_newer"

    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    entry = next(e for e in pulled["time_entries"] if e["id"] == "e3")
    assert entry["deleted_at"] is None


def test_sync_push_entry_without_activity_id_is_invalid():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    result = push_item(
        client,
        token,
        "time_entry",
        {"id": "e4", "started_at": "2026-01-01T08:00:00+00:00"},
    )
    assert result["applied"] == 0
    assert result["conflicts"][0]["resolution"] == "invalid"


def test_sync_category_push_and_pull():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # Push a category.
    result = push_item(
        client,
        token,
        "category",
        {"id": "cat-devb-0001", "name": "Uni", "color": "-16776961", "note": "",
         "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    assert result["applied"] == 1
    assert result["conflicts"] == []

    # Pull returns it with the payload preserved.
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    cat = next(c for c in pulled["categories"] if c["id"] == "cat-devb-0001")
    assert cat["name"] == "Uni"
    assert cat["deleted_at"] is None

    # Tombstone for the known category marks it deleted.
    result = push_item(
        client,
        token,
        "category",
        {"id": "cat-devb-0001",
         "updated_at": "2026-01-02T00:00:00+00:00",
         "deleted_at": "2026-01-02T00:00:00+00:00"},
    )
    assert result["applied"] == 1
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    cat = next(c for c in pulled["categories"] if c["id"] == "cat-devb-0001")
    assert cat["deleted_at"] is not None


def test_sync_tag_push_and_unknown_tombstone():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    result = push_item(
        client,
        token,
        "record_tag",
        {"id": "tag-devb-0001", "name": "Fokus", "icon": "", "color": "-16776961",
         "icon_color_source": 0, "note": "", "archived": False,
         "value_type": "NUMERIC", "value_suffix": "min",
         "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    assert result["applied"] == 1
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    tag = next(t for t in pulled["tags"] if t["id"] == "tag-devb-0001")
    assert tag["value_type"] == "NUMERIC"
    assert tag["deleted_at"] is None

    # Unknown tombstone is a no-op, not an error.
    result = push_item(
        client,
        token,
        "record_tag",
        {"id": "tag-unknown", "updated_at": "2026-01-02T00:00:00+00:00",
         "deleted_at": "2026-01-02T00:00:00+00:00"},
    )
    assert result["applied"] == 1
    assert result["conflicts"] == []


def test_timetable_push_and_pull():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    result = push_item(
        client,
        token,
        "timetable_event",
        {"id": "evt-0001", "name": "Thermo", "day_of_week": 1, "start_time": 495,
         "end_time": 570, "room": "H11", "type": 0, "comment": "",
         "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    assert result["applied"] == 1
    result = push_item(
        client,
        token,
        "subject_goal",
        {"id": "goal-0001", "activity_sync_id": "act-1", "target_seconds": 540000,
         "ects": 5.0, "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    assert result["applied"] == 1

    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    event = next(e for e in pulled["timetable_events"] if e["id"] == "evt-0001")
    assert event["name"] == "Thermo"
    goal = next(g for g in pulled["subject_goals"] if g["id"] == "goal-0001")
    assert goal["target_seconds"] == 540000
    assert goal["deleted_at"] is None


def test_tombstone_not_resurrected_by_stale_push():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # Create, then delete a category.
    push_item(
        client,
        token,
        "category",
        {"id": "cat-res-0001", "name": "Old", "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    push_item(
        client,
        token,
        "category",
        {"id": "cat-res-0001", "updated_at": "2026-01-02T00:00:00+00:00",
         "deleted_at": "2026-01-02T00:00:00+00:00"},
    )

    # A stale non tombstone push with a newer timestamp must not resurrect it.
    result = push_item(
        client,
        token,
        "category",
        {"id": "cat-res-0001", "name": "Old", "updated_at": "2026-01-03T00:00:00+00:00"},
    )
    assert result["conflicts"][0]["resolution"] == "tombstone_kept"
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    cat = next(c for c in pulled["categories"] if c["id"] == "cat-res-0001")
    assert cat["deleted_at"] is not None

    # The same guard applies to timetable entities.
    push_item(
        client,
        token,
        "timetable_todo",
        {"id": "todo-res-0001", "event_sync_id": "evt-1", "date": "2026-01-06",
         "text": "Vorbereitung", "done": 0, "type": 0,
         "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    push_item(
        client,
        token,
        "timetable_todo",
        {"id": "todo-res-0001", "updated_at": "2026-01-02T00:00:00+00:00",
         "deleted_at": "2026-01-02T00:00:00+00:00"},
    )
    result = push_item(
        client,
        token,
        "timetable_todo",
        {"id": "todo-res-0001", "event_sync_id": "evt-1", "date": "2026-01-06",
         "text": "Vorbereitung", "done": 0, "type": 0,
         "updated_at": "2026-01-03T00:00:00+00:00"},
    )
    assert result["conflicts"][0]["resolution"] == "tombstone_kept"
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    todo = next(t for t in pulled["timetable_todos"] if t["id"] == "todo-res-0001")
    assert todo["deleted_at"] is not None


def test_timer_flow_start_stop_current():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # Create an activity
    push_item(
        client,
        token,
        "activity",
        {"id": "act-timer-1", "name": "Lernen", "color": "#4CAF50", "updated_at": "2026-01-01T00:00:00+00:00"},
    )

    # 1. Initially no timer running
    current = client.get("/api/timer/current", headers=auth_headers(token)).json()
    assert current["running"] is False
    assert current["entry"] is None

    # 2. Start timer
    start_resp = client.post(
        "/api/timer/start",
        json={"activity_id": "act-timer-1", "comment": "Mathe Kapitel 3"},
        headers=auth_headers(token),
    )
    assert start_resp.status_code == 200
    started_data = start_resp.json()
    assert started_data["running"] is True
    assert started_data["entry"]["activity_id"] == "act-timer-1"
    assert started_data["entry"]["comment"] == "Mathe Kapitel 3"
    assert started_data["entry"]["ended_at"] is None
    entry_id = started_data["entry"]["id"]

    # 3. GET current should report it running
    current2 = client.get("/api/timer/current", headers=auth_headers(token)).json()
    assert current2["running"] is True
    assert current2["entry"]["id"] == entry_id
    assert current2["entry"]["activity"]["name"] == "Lernen"

    # 4. Stop timer
    stop_resp = client.post(
        "/api/timer/stop",
        headers=auth_headers(token),
    )
    assert stop_resp.status_code == 200
    stop_data = stop_resp.json()
    assert stop_data["running"] is False
    assert stop_data["stopped"]["id"] == entry_id
    assert stop_data["stopped"]["ended_at"] is not None

    # 5. Current timer is now None
    current3 = client.get("/api/timer/current", headers=auth_headers(token)).json()
    assert current3["running"] is False
    assert current3["entry"] is None


def test_timer_start_switches_active_timer():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    push_item(
        client,
        token,
        "activity",
        {"id": "act-timer-a", "name": "Vorlesung", "updated_at": "2026-01-01T00:00:00+00:00"},
    )
    push_item(
        client,
        token,
        "activity",
        {"id": "act-timer-b", "name": "Pause", "updated_at": "2026-01-01T00:00:00+00:00"},
    )

    # Start A
    resp_a = client.post(
        "/api/timer/start",
        json={"activity_id": "act-timer-a"},
        headers=auth_headers(token),
    ).json()
    id_a = resp_a["entry"]["id"]

    # Start B (should auto-stop A)
    resp_b = client.post(
        "/api/timer/start",
        json={"activity_id": "act-timer-b"},
        headers=auth_headers(token),
    ).json()
    id_b = resp_b["entry"]["id"]
    assert id_a != id_b

    # Current should be B
    curr = client.get("/api/timer/current", headers=auth_headers(token)).json()
    assert curr["entry"]["id"] == id_b
    assert curr["entry"]["activity"]["name"] == "Pause"

    # A should now be finished in time entries
    pulled = client.get("/api/sync/pull", headers=auth_headers(token)).json()
    entry_a = next(e for e in pulled["time_entries"] if e["id"] == id_a)
    assert entry_a["ended_at"] is not None

    # Clean up by stopping B
    client.post("/api/timer/stop", headers=auth_headers(token))


def test_timer_stop_when_no_timer_running():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # Make sure no timers running
    client.post("/api/timer/stop", headers=auth_headers(token))

    resp = client.post("/api/timer/stop", headers=auth_headers(token))
    assert resp.status_code == 200
    data = resp.json()
    assert data["running"] is False
    assert data["stopped"] is None


def test_restore_deleted_time_entry_undo():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # 1. Create a time entry
    create_res = client.post(
        "/api/time-entries",
        json={"activity_id": "test_act", "started_at": "2026-10-05T14:00:00Z", "comment": "Original"},
        headers=auth_headers(token),
    )
    assert create_res.status_code == 200
    entry_id = create_res.json()["id"]

    # 2. Delete it (soft delete / tombstone)
    del_res = client.delete(f"/api/time-entries/{entry_id}", headers=auth_headers(token))
    assert del_res.status_code == 200

    # 3. Restore it via POST /api/time-entries with the same ID (Undo behavior)
    restore_res = client.post(
        "/api/time-entries",
        json={"id": entry_id, "activity_id": "test_act", "started_at": "2026-10-05T14:00:00Z", "comment": "Restored"},
        headers=auth_headers(token),
    )
    assert restore_res.status_code == 200, restore_res.text
    restored_data = restore_res.json()
    assert restored_data["id"] == entry_id
    assert restored_data["comment"] == "Restored"
    assert restored_data["deleted_at"] is None

    # Verify active list includes it again
    list_res = client.get("/api/time-entries", headers=auth_headers(token))
    active_ids = [e["id"] for e in list_res.json()]
    assert entry_id in active_ids


def test_bulk_sync_push_and_pull():
    ensure_seed_user()
    client = TestClient(app)
    token = login(client)

    # Push 30 items in a single request (mixed activities, entries, categories, tags, timetable)
    items = []
    # 5 activities
    for i in range(5):
        items.append({
            "entity_type": "activity",
            "data": {
                "id": f"bulk_act_{i}",
                "name": f"Bulk Activity {i}",
                "color_id": i,
                "created_at": "2026-10-06T10:00:00Z",
                "updated_at": "2026-10-06T10:00:00Z",
            },
        })
    # 10 time entries
    for i in range(10):
        items.append({
            "entity_type": "time_entry",
            "data": {
                "id": f"bulk_entry_{i}",
                "activity_id": f"bulk_act_{i % 5}",
                "started_at": f"2026-10-06T{10+i:02d}:00:00Z",
                "ended_at": f"2026-10-06T{10+i:02d}:30:00Z",
                "duration_seconds": 1800,
                "comment": f"Bulk Entry {i}",
                "created_at": "2026-10-06T10:00:00Z",
                "updated_at": "2026-10-06T10:00:00Z",
            },
        })
    # 5 categories
    for i in range(5):
        items.append({
            "entity_type": "category",
            "data": {
                "id": f"bulk_cat_{i}",
                "name": f"Bulk Cat {i}",
                "color": {"colorId": i, "colorInt": ""},
                "created_at": "2026-10-06T10:00:00Z",
                "updated_at": "2026-10-06T10:00:00Z",
            },
        })
    # 5 tags
    for i in range(5):
        items.append({
            "entity_type": "record_tag",
            "data": {
                "id": f"bulk_tag_{i}",
                "name": f"Bulk Tag {i}",
                "color": {"colorId": i, "colorInt": ""},
                "created_at": "2026-10-06T10:00:00Z",
                "updated_at": "2026-10-06T10:00:00Z",
            },
        })
    # 5 timetable events
    for i in range(5):
        items.append({
            "entity_type": "timetable_event",
            "data": {
                "id": f"bulk_evt_{i}",
                "name": f"Bulk Event {i}",
                "day_of_week": (i % 7) + 1,
                "start_time": 480 + i * 60,
                "end_time": 540 + i * 60,
                "created_at": "2026-10-06T10:00:00Z",
                "updated_at": "2026-10-06T10:00:00Z",
            },
        })

    push_res = client.post("/api/sync/push", json={"items": items}, headers=auth_headers(token))
    assert push_res.status_code == 200, push_res.text
    push_data = push_res.json()
    assert push_data["applied"] == 30
    assert len(push_data["conflicts"]) == 0

    # Test incremental delta pull with since
    pull_res = client.get("/api/sync/pull?since=2026-10-06T09:00:00Z", headers=auth_headers(token))
    assert pull_res.status_code == 200
    pull_data = pull_res.json()
    assert len(pull_data["activities"]) >= 5
    assert len(pull_data["time_entries"]) >= 10
    assert len(pull_data["categories"]) >= 5
    assert len(pull_data["tags"]) >= 5
    assert len(pull_data["timetable_events"]) >= 5


