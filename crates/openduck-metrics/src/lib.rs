//! Observability hooks — atomic counters + optional OpenTelemetry OTLP export.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;

static SEAL_MS_SUM: AtomicU64 = AtomicU64::new(0);
static SEAL_COUNT: AtomicU64 = AtomicU64::new(0);
static INIT: OnceLock<()> = OnceLock::new();

/// Initialise OpenTelemetry metrics pipeline.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, an OTLP/gRPC exporter is configured.
/// Otherwise a no-op provider is installed (counters still work via atomics).
pub fn init_metrics() {
    INIT.get_or_init(|| {
        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            if let Ok(exporter) = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .build()
            {
                let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
                    .build();
                let provider = SdkMeterProvider::builder()
                    .with_reader(reader)
                    .build();
                global::set_meter_provider(provider);
                tracing::info!("openduck-metrics: OTLP meter provider installed");
            }
        } else {
            tracing::debug!("openduck-metrics: no OTEL_EXPORTER_OTLP_ENDPOINT, using no-op");
        }
    });
}

/// Record a layer seal duration (both atomics and OTel histogram).
pub fn record_layer_seal_ms(ms: f64) {
    SEAL_MS_SUM.fetch_add(ms as u64, Ordering::Relaxed);
    SEAL_COUNT.fetch_add(1, Ordering::Relaxed);

    let meter = global::meter("openduck");
    let h = meter
        .f64_histogram("openduck.layer.seal_ms")
        .with_description("Time to seal a layer (ms)")
        .build();
    h.record(ms, &[KeyValue::new("component", "diff-metadata")]);
}

/// Total recorded seal milliseconds (test / inspector hooks).
pub fn layer_seal_ms_total() -> u64 {
    SEAL_MS_SUM.load(Ordering::Relaxed)
}

/// Number of seal events recorded via [`record_layer_seal_ms`].
pub fn layer_seal_count() -> u64 {
    SEAL_COUNT.load(Ordering::Relaxed)
}

/// Bounded concurrent fragment executions (hint for gateway worker pools).
pub fn suggested_max_inflight_fragments() -> usize {
    std::env::var("OPENDUCK_MAX_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
}
