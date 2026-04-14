-- Layer reference counting for garbage collection.
-- ref_count tracks how many snapshots reference each layer.
-- Layers with ref_count 0 AND status = 'sealed' are eligible for GC.

ALTER TABLE openduck_layer
    ADD COLUMN IF NOT EXISTS ref_count INTEGER NOT NULL DEFAULT 0;

-- Backfill: count existing snapshot_layer references.
UPDATE openduck_layer l
SET ref_count = sub.cnt
FROM (
    SELECT layer_id, COUNT(*) AS cnt
    FROM openduck_snapshot_layer
    GROUP BY layer_id
) sub
WHERE l.id = sub.layer_id;
