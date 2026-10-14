//! Typed SDK wire protocol errors and request recovery records.

use std::fmt;
use std::time::SystemTime;

use uuid::Uuid;

/// SDK wire protocol error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkErrorCode {
    InvalidInput,
    UnknownOperation,
    NotFound,
    PayloadTooLarge,
    Unauthorized,
    EndpointCredentialForbidden,
    Unavailable,
    /// Requested session state conflicts with an active invocation
    Conflict,
    ConnectionError,
    ConnectionClosed,
    Timeout,
    FrameError,
    CorrelationError,
    ProtocolError,
    ReconnectExhausted,
    UncertainAfterSend,
    Internal,
}

impl SdkErrorCode {
    /// Return the stable wire representation of this error code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::UnknownOperation => "unknown_operation",
            Self::NotFound => "not_found",
            Self::PayloadTooLarge => "payload_too_large",
            Self::Unauthorized => "unauthorized",
            Self::EndpointCredentialForbidden => "endpoint_credential_forbidden",
            Self::Unavailable => "unavailable",
            Self::Conflict => "conflict",
            Self::ConnectionError => "connection_error",
            Self::ConnectionClosed => "connection_closed",
            Self::Timeout => "timeout",
            Self::FrameError => "frame_error",
            Self::CorrelationError => "correlation_error",
            Self::ProtocolError => "protocol_error",
            Self::ReconnectExhausted => "reconnect_exhausted",
            Self::UncertainAfterSend => "uncertain_after_send",
            Self::Internal => "internal_error",
        }
    }

    /// Parse a server error code, mapping unrecognized values to `Internal`.
    pub fn from_wire(value: &str) -> Self {
        match value {
            "invalid_input" => Self::InvalidInput,
            "unknown_operation" => Self::UnknownOperation,
            "not_found" => Self::NotFound,
            "payload_too_large" => Self::PayloadTooLarge,
            "unauthorized" => Self::Unauthorized,
            "endpoint_credential_forbidden" => Self::EndpointCredentialForbidden,
            "unavailable" => Self::Unavailable,
            "conflict" => Self::Conflict,
            "connection_error" => Self::ConnectionError,
            "connection_closed" => Self::ConnectionClosed,
            "timeout" => Self::Timeout,
            "frame_error" => Self::FrameError,
            "correlation_error" => Self::CorrelationError,
            "protocol_error" => Self::ProtocolError,
            "reconnect_exhausted" => Self::ReconnectExhausted,
            "uncertain_after_send" => Self::UncertainAfterSend,
            "internal" | "internal_error" => Self::Internal,
            _ => Self::Internal,
        }
    }
}

impl fmt::Display for SdkErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identity retained when a request may have reached the broker but its reply was lost.
///
/// Records deliberately contain no input payload so credentials and other request data
/// are not retained solely for diagnostics. Callers can use the UUID to reconcile the
/// request with an operation-specific lookup before deciding whether to retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkSentRecord {
    pub id: Uuid,
    pub operation: Option<String>,
    pub sent_at: SystemTime,
}

/// SDK wire protocol error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkError {
    pub code: SdkErrorCode,
    pub message: String,
    /// Original server code when it is not represented by `SdkErrorCode`.
    pub remote_code: Option<String>,
    /// Present only when delivery may have happened before the error was observed.
    pub sent_record: Option<SdkSentRecord>,
}

impl SdkError {
    pub fn new(code: SdkErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            remote_code: None,
            sent_record: None,
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::InvalidInput, message)
    }

    pub fn unknown_operation(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::UnknownOperation, message)
    }

    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::PayloadTooLarge, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Unauthorized, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Unavailable, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Conflict, message)
    }

    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::ConnectionError, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Timeout, message)
    }

    pub fn frame_error(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::FrameError, message)
    }

    pub fn correlation_error(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::CorrelationError, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Internal, message)
    }

    pub fn uncertain_after_send(record: SdkSentRecord) -> Self {
        let mut error = Self::new(
            SdkErrorCode::UncertainAfterSend,
            "SDK request outcome is uncertain after the frame was sent.",
        );
        error.sent_record = Some(record);
        error
    }

    pub fn from_remote(code: &str, message: impl Into<String>) -> Self {
        let mut error = Self::new(SdkErrorCode::from_wire(code), message);
        if error.code == SdkErrorCode::Internal && code != "internal" && code != "internal_error" {
            error.remote_code = Some(code.to_string());
        }
        error
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for SdkError {}

/// Result type for SDK operations.
pub type SdkResult<T> = Result<T, SdkError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_formats_code_and_message() {
        let err = SdkError::invalid_input("test message");
        assert_eq!(err.to_string(), "invalid_input: test message");
    }

    #[test]
    fn error_code_display_works() {
        assert_eq!(SdkErrorCode::InvalidInput.to_string(), "invalid_input");
        assert_eq!(
            SdkErrorCode::PayloadTooLarge.to_string(),
            "payload_too_large"
        );
        assert_eq!(SdkErrorCode::Unauthorized.to_string(), "unauthorized");
        assert_eq!(
            SdkErrorCode::from_wire("endpoint_credential_forbidden"),
            SdkErrorCode::EndpointCredentialForbidden
        );
        assert_eq!(
            SdkErrorCode::UncertainAfterSend.to_string(),
            "uncertain_after_send"
        );
    }

    #[test]
    fn conflict_error_code_round_trips_on_the_wire() {
        let error = SdkError::conflict("session is active");
        assert_eq!(error.code.to_string(), "conflict");
        assert_eq!(SdkErrorCode::from_wire("conflict"), SdkErrorCode::Conflict);
    }

    #[test]
    fn unknown_remote_code_is_preserved_with_typed_internal_code() {
        let error = SdkError::from_remote("future_error", "failed");
        assert_eq!(error.code, SdkErrorCode::Internal);
        assert_eq!(error.remote_code.as_deref(), Some("future_error"));
    }
}
