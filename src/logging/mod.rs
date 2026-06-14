pub mod metrics_layer;
pub mod metrics;

use tracing_subscriber::{filter::{EnvFilter, FilterFn}, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub fn initialize_logging(metrics_path: String) {
    let console_layer = tracing_subscriber::fmt::layer()
        .with_filter(EnvFilter::new("info"))
        .with_filter(FilterFn::new(|metadata| {
            metadata.target() != "metrics"
        }));

    // Our custom metrics file layer
    let metrics_layer = metrics_layer::MetricsLayer::new(&metrics_path);

    tracing_subscriber::registry()
        .with(console_layer)
        .with(metrics_layer)
        .init();
}
