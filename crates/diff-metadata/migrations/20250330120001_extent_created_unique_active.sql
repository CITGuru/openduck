-- Extent ordering for overlapping writes; single active layer per database.

ALTER TABLE openduck_extent
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_layer_per_db
    ON openduck_layer (db_id)
    WHERE status = 'active';
