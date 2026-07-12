//! Outbound webhooks — an [`EventSink`](crate::EventSink) that relays engine
//! lifecycle events to a configured HTTP endpoint.
//!
//! The event bus that feeds auto-versioning ([`crate::auto_version`]) is also
//! the substrate for external integrations: a deployment names an endpoint and
//! this sink POSTs each room-bearing lifecycle event to it as JSON, so an
//! outside system reacts to a version capture, a compaction, a subscribe, a
//! restore, or a publish without polling.
//!
//! A sink is a passive observer of the commit path, so delivery never blocks or
//! panics the engine: [`on_event`](WebhookSink::on_event) only serializes the
//! event and offers it to a bounded queue, and a separate task owns the HTTP
//! client and drains the queue off the hot path. Delivery is best-effort — a
//! full queue (a slow or failing endpoint that has fallen behind) drops the
//! event rather than stalling emission, a failed POST is logged and not retried,
//! and no ordering is preserved across a restart. The receiver authenticates a
//! POST with the optional shared secret in the [`SECRET_HEADER`], sent over TLS
//! and never placed in the body.

use serde::Serialize;
use tokio::sync::mpsc::{self, Sender};

use crate::{EngineEvent, EventSink};

/// How many serialized events may await delivery before the sink drops further
/// events — a bound on memory when the endpoint falls behind.
const QUEUE_CAPACITY: usize = 1024;

/// How long a single POST may take before it is abandoned, bounding how long one
/// wedged request holds up the events queued behind it.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The header carrying the configured shared secret for the receiver to
/// authenticate the POST. Sent verbatim, so it belongs over TLS; never in the
/// body.
pub const SECRET_HEADER: &str = "X-Crdtsync-Webhook-Secret";

/// Where and how to deliver webhook events: the endpoint URL and an optional
/// shared secret the receiver checks.
#[derive(Clone)]
pub struct WebhookConfig {
    pub url: String,
    pub secret: Option<String>,
}

/// The JSON body POSTed for one event: a `type` tag plus that event's fields.
/// Byte-valued names (room, version, branch) render as UTF-8 text, lossily for a
/// non-UTF-8 name — as the rest of the server surfaces names.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum WebhookBody {
    Subscribed {
        room: String,
    },
    VersionCreated {
        room: String,
        name: String,
    },
    VersionRenamed {
        room: String,
        from: String,
        to: String,
    },
    VersionDeleted {
        room: String,
        name: String,
    },
    Compacted {
        room: String,
        floor: u64,
    },
    AfterRestore {
        room: String,
        branch: String,
    },
    BeforePublish {
        room: String,
        branch: String,
    },
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The webhook body for a lifecycle event, or `None` for a roomless transport
/// event (connect/disconnect) or the reserved migration event — a webhook
/// relays the room-bearing lifecycle moments, mirroring the auto-version sink's
/// filter.
fn body(event: &EngineEvent) -> Option<WebhookBody> {
    Some(match *event {
        EngineEvent::Subscribed { room, .. } => WebhookBody::Subscribed { room: text(room) },
        EngineEvent::VersionCreated { room, name } => WebhookBody::VersionCreated {
            room: text(room),
            name: text(name),
        },
        EngineEvent::VersionRenamed { room, from, to } => WebhookBody::VersionRenamed {
            room: text(room),
            from: text(from),
            to: text(to),
        },
        EngineEvent::VersionDeleted { room, name } => WebhookBody::VersionDeleted {
            room: text(room),
            name: text(name),
        },
        EngineEvent::Compacted { room, floor } => WebhookBody::Compacted {
            room: text(room),
            floor,
        },
        EngineEvent::AfterRestore { room, branch } => WebhookBody::AfterRestore {
            room: text(room),
            branch: text(branch),
        },
        EngineEvent::BeforePublish { room, branch } => WebhookBody::BeforePublish {
            room: text(room),
            branch: text(branch),
        },
        _ => return None,
    })
}

/// The webhook sink held in the hub's sink list. It serializes each event and
/// offers it to the delivery worker over a bounded channel, never blocking the
/// commit path. Build it with [`spawn`](WebhookSink::spawn), which starts the
/// worker on the current tokio runtime.
pub struct WebhookSink {
    tx: Sender<WebhookBody>,
}

impl WebhookSink {
    /// Build a sink and spawn its delivery worker on the current tokio runtime.
    /// The worker owns the HTTP client and drains the queue; the returned sink
    /// only feeds it, so it must be constructed from within a runtime with I/O
    /// enabled.
    pub fn spawn(config: WebhookConfig) -> WebhookSink {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        tokio::spawn(deliver(rx, config));
        WebhookSink { tx }
    }
}

impl EventSink for WebhookSink {
    fn on_event(&self, event: &EngineEvent) {
        if let Some(body) = body(event) {
            // Best-effort: a full queue (the endpoint has fallen behind) drops
            // this event rather than blocking the engine's emit.
            let _ = self.tx.try_send(body);
        }
    }
}

/// Drain the queue, POSTing each event as JSON to the configured endpoint. A
/// client-build or send failure is logged, never fatal — the engine keeps
/// running and the next event is still attempted.
async fn deliver(mut rx: mpsc::Receiver<WebhookBody>, config: WebhookConfig) {
    let client = match reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build() {
        Ok(client) => client,
        Err(e) => {
            eprintln!("crdtsync: webhook could not build HTTP client: {e}");
            return;
        }
    };
    while let Some(body) = rx.recv().await {
        let mut request = client.post(&config.url).json(&body);
        if let Some(secret) = &config.secret {
            request = request.header(SECRET_HEADER, secret);
        }
        // A transport error, a timeout, or a non-2xx status is a failed delivery:
        // logged, never retried.
        if let Err(e) = request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            eprintln!("crdtsync: webhook POST to {} failed: {e}", config.url);
        }
    }
}
