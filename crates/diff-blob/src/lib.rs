//! Object store backend for sealed layers (S3-compatible via object_store).

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
}

/// Upload a sealed layer blob; returns storage URI you persist in metadata.
pub async fn put_layer_bytes(
    store: &dyn ObjectStore,
    key: &str,
    data: Bytes,
) -> Result<String, BlobError> {
    let path = ObjectPath::from(key);
    store.put(&path, PutPayload::from_bytes(data)).await?;
    Ok(format!("s3://{key}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[tokio::test]
    async fn in_memory_put() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = put_layer_bytes(store.as_ref(), "db1/layer1", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert!(uri.contains("layer1"));
    }
}
