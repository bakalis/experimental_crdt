/// Emit a structured metrics event. All key-value pairs are written
/// as fields in the JSON output.
///
/// Usage:
///   metric!(event = "request", endpoint = "/api/users", duration_ms = 42, status = 200);
#[macro_export]
macro_rules! metric {
    ($($key:ident = $val:expr),+ $(,)?) => {
        tracing::trace!(target: "metrics", $($key = $val),+);
    };
}
