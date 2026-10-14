//! Typed frame structures and JSON serialization for the SDK wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::error::{SdkError, SdkResult};

/// Protocol version implemented by this SDK client.
pub const PROTOCOL_VERSION: u32 = 3;

/// Maximum UTF-8 byte length for a single broker JSON frame.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// Known frame type identifiers in the SDK wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Hello,
    ServerHello,
    BrokerHello,
    BrokerRequest,
    BrokerResponse,
    QueryRequest,
    QueryResponse,
    ControlRequest,
    ControlResponse,
    EventReplay,
    EventReplayResult,
    SessionActivate,
    SessionActivateResult,
}

impl FrameType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::ServerHello => "server_hello",
            Self::BrokerHello => "broker_hello",
            Self::BrokerRequest => "broker_request",
            Self::BrokerResponse => "broker_response",
            Self::QueryRequest => "query_request",
            Self::QueryResponse => "query_response",
            Self::ControlRequest => "control_request",
            Self::ControlResponse => "control_response",
            Self::EventReplay => "event_replay",
            Self::EventReplayResult => "event_replay_result",
            Self::SessionActivate => "session_activate",
            Self::SessionActivateResult => "session_activate_result",
        }
    }

    pub fn parse(value: &str) -> SdkResult<Self> {
        match value {
            "hello" => Ok(Self::Hello),
            "server_hello" => Ok(Self::ServerHello),
            "broker_hello" => Ok(Self::BrokerHello),
            "broker_request" => Ok(Self::BrokerRequest),
            "broker_response" => Ok(Self::BrokerResponse),
            "query_request" => Ok(Self::QueryRequest),
            "query_response" => Ok(Self::QueryResponse),
            "control_request" => Ok(Self::ControlRequest),
            "control_response" => Ok(Self::ControlResponse),
            "event_replay" => Ok(Self::EventReplay),
            "event_replay_result" => Ok(Self::EventReplayResult),
            "session_activate" => Ok(Self::SessionActivate),
            "session_activate_result" => Ok(Self::SessionActivateResult),
            _ => Err(SdkError::frame_error(format!(
                "unknown frame type: {value}"
            ))),
        }
    }
}

/// A typed JSON-object frame in the SDK wire protocol.
///
/// The map remains open-ended to allow operation-specific fields while the frame type,
/// correlation UUID, and handshake are validated by the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SdkFrame {
    /// Underlying JSON object.
    pub data: Map<String, Value>,
}

impl SdkFrame {
    /// Create a frame from a JSON object.
    pub fn new(data: Map<String, Value>) -> Self {
        Self { data }
    }

    /// Create an empty frame object.
    pub fn empty() -> Self {
        Self::new(Map::new())
    }

    /// Parse and validate the frame's `type` field.
    pub fn frame_type(&self) -> SdkResult<FrameType> {
        let frame_type = self
            .data
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| SdkError::frame_error("missing 'type' field"))?;
        FrameType::parse(frame_type)
    }

    /// Get the optional UUID used for request/response correlation.
    pub fn id(&self) -> SdkResult<Option<Uuid>> {
        match self.data.get("id") {
            None => Ok(None),
            Some(Value::String(id)) => Uuid::parse_str(id).map(Some).map_err(|error| {
                SdkError::correlation_error(format!("invalid frame UUID: {error}"))
            }),
            Some(_) => Err(SdkError::correlation_error(
                "frame 'id' field must be a UUID string",
            )),
        }
    }

    /// Set the frame's correlation UUID.
    pub fn set_id(&mut self, id: Uuid) {
        self.data
            .insert("id".to_string(), Value::String(id.to_string()));
    }

    /// Read a response correlation UUID from either supported field name.
    pub fn response_id(&self) -> SdkResult<Option<Uuid>> {
        if self.data.contains_key("id") {
            return self.id();
        }
        match self.data.get("requestId") {
            None => Ok(None),
            Some(Value::String(id)) => Uuid::parse_str(id).map(Some).map_err(|error| {
                SdkError::correlation_error(format!("invalid response UUID: {error}"))
            }),
            Some(_) => Err(SdkError::correlation_error(
                "frame 'requestId' field must be a UUID string",
            )),
        }
    }

    /// Read the `ok` boolean from a response frame.
    pub fn ok(&self) -> SdkResult<bool> {
        self.data
            .get("ok")
            .and_then(Value::as_bool)
            .ok_or_else(|| SdkError::frame_error("missing or invalid 'ok' field"))
    }

    /// Read the remote error code and message from an error response.
    pub fn error(&self) -> Option<SdkResult<(String, String)>> {
        let error = self.data.get("error")?.as_object()?;
        let code = error.get("code")?.as_str()?.to_string();
        let message = error.get("message")?.as_str()?.to_string();
        Some(Ok((code, message)))
    }

    /// Get the response result field.
    pub fn result(&self) -> Option<Value> {
        self.data.get("result").cloned()
    }

    /// Serialize this frame to compact JSON.
    pub fn to_json(&self) -> SdkResult<String> {
        serde_json::to_string(&self.data)
            .map_err(|error| SdkError::frame_error(format!("JSON serialization error: {error}")))
    }

    /// Parse a JSON object as an SDK frame.
    pub fn from_json(json: &str) -> SdkResult<Self> {
        match serde_json::from_str::<Value>(json)
            .map_err(|error| SdkError::frame_error(format!("JSON parsing error: {error}")))?
        {
            Value::Object(data) => Ok(Self::new(data)),
            _ => Err(SdkError::frame_error("SDK frame must be a JSON object")),
        }
    }

    /// Create the protocol-v3 client hello with the advertised capabilities.
    pub fn client_hello(capabilities: &[String]) -> Self {
        let mut data = Map::new();
        data.insert("type".to_string(), Value::String("hello".to_string()));
        data.insert(
            "protocolVersion".to_string(),
            Value::Number(PROTOCOL_VERSION.into()),
        );
        data.insert(
            "capabilities".to_string(),
            Value::Array(capabilities.iter().cloned().map(Value::String).collect()),
        );
        Self::new(data)
    }

    /// Return whether this frame completes a server hello handshake.
    pub fn is_server_hello(&self) -> bool {
        matches!(
            self.data.get("type").and_then(Value::as_str),
            Some("hello" | "server_hello" | "broker_hello")
        )
    }

    /// Read an optional connection identifier from a server hello.
    pub fn connection_id(&self) -> Option<&str> {
        self.data.get("connectionId").and_then(Value::as_str)
    }

    /// Create a broker hello frame for protocol tests and mock servers.
    pub fn broker_hello() -> Self {
        let mut data = Map::new();
        data.insert(
            "type".to_string(),
            Value::String("broker_hello".to_string()),
        );
        data.insert(
            "protocolVersion".to_string(),
            Value::Number(PROTOCOL_VERSION.into()),
        );
        Self::new(data)
    }

    /// Create a broker operation request frame.
    pub fn broker_request(id: Uuid, operation: String, input: Value) -> Self {
        let mut data = Map::new();
        data.insert(
            "type".to_string(),
            Value::String("broker_request".to_string()),
        );
        data.insert("id".to_string(), Value::String(id.to_string()));
        data.insert("operation".to_string(), Value::String(operation));
        data.insert("input".to_string(), input);
        Self::new(data)
    }

    /// Create a successful broker response frame.
    pub fn broker_response_ok(id: Uuid, result: Value) -> Self {
        let mut data = Map::new();
        data.insert(
            "type".to_string(),
            Value::String("broker_response".to_string()),
        );
        data.insert("id".to_string(), Value::String(id.to_string()));
        data.insert("ok".to_string(), Value::Bool(true));
        data.insert("result".to_string(), result);
        Self::new(data)
    }

    /// Create an error broker response frame.
    pub fn broker_response_error(id: Uuid, code: String, message: String) -> Self {
        let mut error = Map::new();
        error.insert("code".to_string(), Value::String(code));
        error.insert("message".to_string(), Value::String(message));

        let mut data = Map::new();
        data.insert(
            "type".to_string(),
            Value::String("broker_response".to_string()),
        );
        data.insert("id".to_string(), Value::String(id.to_string()));
        data.insert("ok".to_string(), Value::Bool(false));
        data.insert("error".to_string(), Value::Object(error));
        Self::new(data)
    }

    /// Get the serialized frame length in UTF-8 bytes.
    pub fn byte_size(&self) -> usize {
        self.to_json().map_or(0, |json| json.len())
    }

    /// Read the operation name from a request frame.
    pub fn operation(&self) -> Option<String> {
        self.data
            .get("operation")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    /// Read the request input object.
    pub fn input(&self) -> Option<Value> {
        self.data.get("input").cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_type_round_trip() {
        assert_eq!(FrameType::Hello.as_str(), "hello");
        assert_eq!(
            FrameType::parse("broker_hello").unwrap(),
            FrameType::BrokerHello
        );
    }

    #[test]
    fn frame_parsing_extracts_type() {
        assert_eq!(
            SdkFrame::broker_hello().frame_type().unwrap(),
            FrameType::BrokerHello
        );
    }

    #[test]
    fn frame_id_round_trips_as_a_uuid() {
        let id = Uuid::new_v4();
        let frame = SdkFrame::broker_request(id, "test.operation".to_string(), Value::Null);
        assert_eq!(frame.id().unwrap(), Some(id));
    }

    #[test]
    fn json_round_trip_preserves_the_typed_frame() {
        let frame = SdkFrame::client_hello(&["capability.one".to_string()]);
        let parsed = SdkFrame::from_json(&frame.to_json().unwrap()).unwrap();
        assert_eq!(parsed, frame);
        assert_eq!(parsed.frame_type().unwrap(), FrameType::Hello);
    }

    #[test]
    fn client_hello_advertises_protocol_and_capabilities() {
        let frame = SdkFrame::client_hello(&["capability.one".to_string()]);
        assert_eq!(frame.data["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(
            frame.data["capabilities"],
            serde_json::json!(["capability.one"])
        );
    }

    #[test]
    fn broker_error_response_exposes_code_and_message() {
        let frame = SdkFrame::broker_response_error(
            Uuid::new_v4(),
            "invalid_input".to_string(),
            "bad input".to_string(),
        );
        assert_eq!(
            frame.error().unwrap().unwrap(),
            ("invalid_input".into(), "bad input".into())
        );
        assert!(!frame.ok().unwrap());
    }

    #[test]
    fn operation_and_input_are_recoverable() {
        let input = serde_json::json!({"session_id": "session-1"});
        let frame = SdkFrame::broker_request(
            Uuid::new_v4(),
            "query.get_session".to_string(),
            input.clone(),
        );
        assert_eq!(frame.operation().as_deref(), Some("query.get_session"));
        assert_eq!(frame.input(), Some(input));
    }

    #[test]
    fn malformed_json_and_non_object_values_are_rejected() {
        assert!(SdkFrame::from_json("{").is_err());
        assert!(SdkFrame::from_json("[]").is_err());
        assert!(SdkFrame::from_json("null").is_err());
    }

    #[test]
    fn invalid_correlation_uuid_is_rejected() {
        let frame = SdkFrame::from_json(r#"{"type":"broker_response","id":"not-a-uuid"}"#).unwrap();
        assert_eq!(
            frame.id().unwrap_err().code,
            crate::error::SdkErrorCode::CorrelationError
        );
    }
}
