-- OpenDuck differential storage metadata (v1)

CREATE TABLE openduck_db (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    current_tip_snapshot_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE openduck_snapshot (
    id UUID PRIMARY KEY,
    db_id UUID NOT NULL REFERENCES openduck_db(id) ON DELETE CASCADE,
    parent_snapshot_id UUID REFERENCES openduck_snapshot(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE openduck_layer (
    id UUID PRIMARY KEY,
    db_id UUID NOT NULL REFERENCES openduck_db(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    storage_uri TEXT NOT NULL,
    byte_len BIGINT NOT NULL DEFAULT 0,
    content_hash TEXT,
    sealed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('active', 'sealed'))
);

CREATE INDEX idx_layer_db_status ON openduck_layer(db_id, status);

CREATE TABLE openduck_snapshot_layer (
    snapshot_id UUID NOT NULL REFERENCES openduck_snapshot(id) ON DELETE CASCADE,
    layer_id UUID NOT NULL REFERENCES openduck_layer(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, ordinal)
);

CREATE TABLE openduck_extent (
    id UUID PRIMARY KEY,
    db_id UUID NOT NULL REFERENCES openduck_db(id) ON DELETE CASCADE,
    layer_id UUID NOT NULL REFERENCES openduck_layer(id) ON DELETE CASCADE,
    logical_start BIGINT NOT NULL,
    length BIGINT NOT NULL,
    phys_start BIGINT NOT NULL,
    superseded_by UUID REFERENCES openduck_extent(id),
    snapshot_id UUID REFERENCES openduck_snapshot(id)
);

CREATE INDEX idx_extent_db_logical ON openduck_extent(db_id, logical_start)
    WHERE superseded_by IS NULL;

CREATE TABLE openduck_write_lease (
    db_id UUID PRIMARY KEY REFERENCES openduck_db(id) ON DELETE CASCADE,
    holder TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
