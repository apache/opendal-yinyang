// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Process-local HTTP counters for native Linux benchmarks.
//!
//! This instrumentation is compiled only for the `benchmark-telemetry` feature.
//! It records the released OpenDAL metrics layer's HTTP counters without
//! terminating TLS or adding a proxy to the measured network path.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use opendal::Operator;
use serde::Serialize;

const SNAPSHOT_SCHEMA: &str = "yinyang.benchmark-http.v1";
const REQUEST_DURATION: &str = "opendal_http_request_duration_seconds";
const REQUEST_BYTES: &str = "opendal_http_request_bytes";
const RESPONSE_BYTES: &str = "opendal_http_response_bytes";
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(50);

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Owns the benchmark recorder and its periodic snapshot writer.
pub struct Telemetry {
    stop: Arc<AtomicBool>,
    writer: Option<JoinHandle<()>>,
}

impl Telemetry {
    /// Install telemetry when `YINYANG_BENCH_METRICS_FILE` names an output file.
    pub fn install() -> Option<Self> {
        let path = std::env::var_os("YINYANG_BENCH_METRICS_FILE").map(std::path::PathBuf::from)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create benchmark telemetry directory");
        }
        let state = Arc::new(TelemetryState::default());
        metrics::set_global_recorder(TelemetryRecorder {
            state: state.clone(),
        })
        .expect("install benchmark metrics recorder");
        ENABLED.store(true, Ordering::SeqCst);
        write_snapshot(&path, &state);

        let stop = Arc::new(AtomicBool::new(false));
        let writer_state = state;
        let writer_stop = stop.clone();
        let writer = thread::spawn(move || {
            while !writer_stop.load(Ordering::SeqCst) {
                thread::sleep(SNAPSHOT_INTERVAL);
                write_snapshot(&path, &writer_state);
            }
            write_snapshot(&path, &writer_state);
        });
        Some(Self {
            stop,
            writer: Some(writer),
        })
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::SeqCst);
        self.stop.store(true, Ordering::SeqCst);
        if let Some(writer) = self.writer.take() {
            writer.join().expect("join benchmark telemetry writer");
        }
    }
}

/// Apply benchmark instrumentation to an operator when explicitly enabled.
pub fn instrument(operator: Operator) -> Operator {
    if ENABLED.load(Ordering::SeqCst) {
        operator.layer(opendal::layers::MetricsLayer::new())
    } else {
        operator
    }
}

#[derive(Default)]
struct TelemetryState {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    read_requests: AtomicU64,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    schema: &'a str,
    requests: u64,
    request_bytes: u64,
    response_bytes: u64,
    read_requests: u64,
}

fn write_snapshot(path: &Path, state: &TelemetryState) {
    let snapshot = Snapshot {
        schema: SNAPSHOT_SCHEMA,
        requests: state.requests.load(Ordering::Relaxed),
        request_bytes: state.request_bytes.load(Ordering::Relaxed),
        response_bytes: state.response_bytes.load(Ordering::Relaxed),
        read_requests: state.read_requests.load(Ordering::Relaxed),
    };
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    if let Ok(bytes) = serde_json::to_vec(&snapshot)
        && fs::write(&temporary, bytes).is_ok()
    {
        let _ = fs::rename(&temporary, path);
    }
}

struct TelemetryRecorder {
    state: Arc<TelemetryState>,
}

impl Recorder for TelemetryRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        match key.name() {
            REQUEST_DURATION => Histogram::from_arc(Arc::new(RequestHistogram {
                state: self.state.clone(),
                read: key
                    .labels()
                    .any(|label| label.key() == "operation" && label.value() == "read"),
            })),
            REQUEST_BYTES => Histogram::from_arc(Arc::new(ByteHistogram {
                state: self.state.clone(),
                response: false,
            })),
            RESPONSE_BYTES => Histogram::from_arc(Arc::new(ByteHistogram {
                state: self.state.clone(),
                response: true,
            })),
            _ => Histogram::noop(),
        }
    }
}

struct RequestHistogram {
    state: Arc<TelemetryState>,
    read: bool,
}

impl HistogramFn for RequestHistogram {
    fn record(&self, _: f64) {
        self.state.requests.fetch_add(1, Ordering::Relaxed);
        if self.read {
            self.state.read_requests.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct ByteHistogram {
    state: Arc<TelemetryState>,
    response: bool,
}

impl HistogramFn for ByteHistogram {
    fn record(&self, value: f64) {
        let value = if value.is_finite() && value > 0.0 {
            value.min(u64::MAX as f64) as u64
        } else {
            0
        };
        let counter = if self.response {
            &self.state.response_bytes
        } else {
            &self.state.request_bytes
        };
        counter.fetch_add(value, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{Key, Label, Level};

    #[test]
    fn recorder_counts_requests_bytes_and_read_operations() {
        let state = Arc::new(TelemetryState::default());
        let recorder = TelemetryRecorder {
            state: state.clone(),
        };
        let metadata = Metadata::new("test", Level::INFO, Some("test"));
        let key = Key::from_parts(
            REQUEST_DURATION,
            vec![Label::new("operation", "read"), Label::new("other", "x")],
        );
        recorder.register_histogram(&key, &metadata).record(0.1);
        recorder
            .register_histogram(&Key::new(REQUEST_BYTES), &metadata)
            .record(12.0);
        recorder
            .register_histogram(&Key::new(RESPONSE_BYTES), &metadata)
            .record(34.0);

        assert_eq!(state.requests.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_requests.load(Ordering::Relaxed), 1);
        assert_eq!(state.request_bytes.load(Ordering::Relaxed), 12);
        assert_eq!(state.response_bytes.load(Ordering::Relaxed), 34);
    }
}
