//! GJC SDK wire transport implementation.
//!
//! This crate provides a Rust implementation of the GJC SDK wire protocol,
//! compatible with the gajae-code SDK broker framing, request/response correlation,
//! and event stream semantics.

#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod error;
pub mod frame;
pub mod registry;
pub mod session;
pub mod transport;

pub use error::{SdkError, SdkErrorCode, SdkResult, SdkSentRecord};
pub use frame::{FrameType, SdkFrame};
pub use registry::{OperationMetadata, OperationRegistry, Schema};
pub use session::{
    InvocationOutcome, InvocationReconciliationStore, InvocationRecord, InvocationStatus,
    RuntimeSessionRunner, SessionDispatch, SessionEvent, SessionEventStream, SessionEventType,
    SessionInvocation, SessionPage, SessionRouter, SessionRunResult, SessionSummary,
    SessionTurnRunner, TerminalScopeRecord, TerminalScopeStore,
};
pub use transport::{SdkClient, SdkClientConfig};
