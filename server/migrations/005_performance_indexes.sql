-- Version 5: composite performance indexes for sync pull and running timer queries.
CREATE INDEX IF NOT EXISTS ix_activities_user_updated ON activities(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_time_entries_user_updated ON time_entries(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_time_entries_user_running ON time_entries(user_id, ended_at, deleted_at);
CREATE INDEX IF NOT EXISTS ix_goals_user_updated ON goals(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_sync_categories_user_updated ON sync_categories(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_sync_tags_user_updated ON sync_tags(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_sync_timetable_user_updated ON sync_timetable(user_id, updated_at);
CREATE INDEX IF NOT EXISTS ix_sync_timetable_user_type ON sync_timetable(user_id, entity_type);
