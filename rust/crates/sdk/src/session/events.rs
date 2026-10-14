//! Event stream types for session lifecycle operations.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Lifecycle events published for one SDK turn invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventType {
    TurnAccepted,
    AgentBegin,
    ToolUse,
    ToolResult,
    AgentEnd,
    TurnSteered,
    TurnEnded,
}

/// One ordered session lifecycle event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEvent {
    pub event_id: String,
    pub event_type: SessionEventType,
    pub session_id: String,
    pub invocation_id: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub durable: bool,
    pub payload: Value,
}
