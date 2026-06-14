use std::{
    fs::{File, OpenOptions},
    io::Write,
    sync::Mutex,
};
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

pub struct MetricsLayer {
    file: Mutex<File>,
}

impl MetricsLayer {
    pub fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("Failed to open metrics file");

        MetricsLayer {
            file: Mutex::new(file),
        }
    }
}

impl<S: Subscriber> Layer<S> for MetricsLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "metrics" {
            return;
        }

        let mut visitor = MetricsVisitor::default();
        event.record(&mut visitor);

        let entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": event.metadata().level().as_str(),
            "fields": visitor.fields,
        });

        if let Ok(mut file) = self.file.lock() {
            writeln!(file, "{}", entry).ok();
        }
    }
}

/// Visits tracing event fields and collects them into a JSON map.
#[derive(Default)]
struct MetricsVisitor {
    fields: serde_json::Map<String, serde_json::Value>,
}

impl tracing::field::Visit for MetricsVisitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields.insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(field.name().to_string(), serde_json::json!(format!("{:?}", value)));
    }
}
