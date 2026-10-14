//! Durable SDK session routing and turn lifecycle operations.

pub mod events;
pub mod reconciliation;
pub mod terminal_scope;

pub use events::{SessionEvent, SessionEventType};
pub use reconciliation::{
    InvocationOutcome, InvocationReconciliationStore, InvocationRecord, InvocationStatus,
};
pub use terminal_scope::{TerminalScopeRecord, TerminalScopeStore};

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use runtime::{
    ApiClient, ConversationEvent, ConversationRuntime, Session, SessionStore, ToolExecutor,
    TurnControlSignal,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::error::{SdkError, SdkResult};
use crate::frame::{FrameType, SdkFrame};

const DEFAULT_PAGE_SIZE: usize = 20;
const MAX_PAGE_SIZE: usize = 100;
const MAX_SESSION_ID_BYTES: usize = 128;
const MAX_CORRELATION_KEY_BYTES: usize = 512;

/// Result returned by one session turn implementation.
pub enum SessionRunResult {
    Completed(Session),
    Aborted(Session),
    Failed { session: Session, message: String },
}

/// Pluggable synchronous turn driver used by [`SessionRouter`].
pub trait SessionTurnRunner: Send + Sync + 'static {
    fn run(
        &self,
        session: Session,
        prompt: String,
        control: TurnControlSignal,
        event_callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult;
}

/// Bridges a runtime conversation into the SDK session router.
pub struct RuntimeSessionRunner<C, T> {
    build_runtime: Arc<dyn Fn(Session) -> ConversationRuntime<C, T> + Send + Sync>,
    marker: std::marker::PhantomData<fn() -> (C, T)>,
}

impl<C, T> RuntimeSessionRunner<C, T>
where
    C: ApiClient + 'static,
    T: ToolExecutor + 'static,
{
    /// Construct a runner from a factory that supplies the API and tool clients.
    pub fn new<F>(build_runtime: F) -> Self
    where
        F: Fn(Session) -> ConversationRuntime<C, T> + Send + Sync + 'static,
    {
        Self {
            build_runtime: Arc::new(build_runtime),
            marker: std::marker::PhantomData,
        }
    }
}

impl<C, T> SessionTurnRunner for RuntimeSessionRunner<C, T>
where
    C: ApiClient + 'static,
    T: ToolExecutor + 'static,
{
    fn run(
        &self,
        session: Session,
        mut prompt: String,
        control: TurnControlSignal,
        event_callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        let mut runtime = (self.build_runtime)(session)
            .with_turn_control_signal(control.clone())
            .with_event_callback(event_callback);

        loop {
            match runtime.run_turn(prompt, None) {
                Ok(summary) if summary.interrupted || control.is_aborted() => {
                    return SessionRunResult::Aborted(runtime.into_session());
                }
                Ok(_) => match control.finish_or_take_steer() {
                    Some(steer) => prompt = steer,
                    None => return SessionRunResult::Completed(runtime.into_session()),
                },
                Err(error) => {
                    control.close();
                    return SessionRunResult::Failed {
                        session: runtime.into_session(),
                        message: error.to_string(),
                    };
                }
            }
        }
    }
}

/// One paginated row returned by `turn.list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    pub session_id: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub message_count: usize,
}

/// Page of persisted sessions. `next_cursor` is an opaque decimal offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionPage {
    pub sessions: Vec<SessionSummary>,
    pub next_cursor: Option<String>,
}

/// A started invocation and its ordered event stream.
pub struct SessionInvocation {
    pub session_id: String,
    pub invocation_id: String,
    pub events: SessionEventStream,
}

impl Debug for SessionInvocation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionInvocation")
            .field("session_id", &self.session_id)
            .field("invocation_id", &self.invocation_id)
            .finish_non_exhaustive()
    }
}

/// Receiver for lifecycle events emitted by an invocation.
pub struct SessionEventStream {
    receiver: mpsc::UnboundedReceiver<SessionEvent>,
}

impl Debug for SessionEventStream {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionEventStream")
            .finish_non_exhaustive()
    }
}

impl SessionEventStream {
    /// Receive the next lifecycle event, returning `None` after the stream closes.
    pub async fn recv(&mut self) -> Option<SessionEvent> {
        self.receiver.recv().await
    }
}

/// Result of dispatching one SDK broker request.
pub struct SessionDispatch {
    pub response: SdkFrame,
    pub events: Option<SessionEventStream>,
}

impl Debug for SessionDispatch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionDispatch")
            .field("response", &self.response)
            .field("has_events", &self.events.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct SessionRouter {
    inner: Arc<SessionRouterInner>,
}

struct SessionRouterInner {
    store: SessionStore,
    runner: Arc<dyn SessionTurnRunner>,
    reconciliation: InvocationReconciliationStore,
    terminal_scopes: TerminalScopeStore,
    reconciliation_lock: Mutex<()>,
    active: Mutex<HashMap<String, ActiveInvocation>>,
}

#[derive(Clone)]
struct ActiveInvocation {
    invocation_id: String,
    correlation_key: Option<String>,
    control: TurnControlSignal,
    event_publisher: Arc<SessionEventPublisher>,
    done: watch::Receiver<bool>,
}

struct SessionEventPublisher {
    session_id: String,
    invocation_id: String,
    sequence: std::sync::atomic::AtomicU64,
    publication_lock: Mutex<()>,
    sender: mpsc::UnboundedSender<SessionEvent>,
}

impl SessionEventPublisher {
    fn new(
        session_id: String,
        invocation_id: String,
        sender: mpsc::UnboundedSender<SessionEvent>,
    ) -> Self {
        Self {
            session_id,
            invocation_id,
            sequence: std::sync::atomic::AtomicU64::new(0),
            publication_lock: Mutex::new(()),
            sender,
        }
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.publication_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn build_locked(
        &self,
        event_type: SessionEventType,
        payload: Value,
        durable: bool,
    ) -> SessionEvent {
        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(1);
        SessionEvent {
            event_id: format!("{}:{sequence}", self.invocation_id),
            event_type,
            session_id: self.session_id.clone(),
            invocation_id: self.invocation_id.clone(),
            sequence,
            timestamp_ms: now_ms(),
            durable,
            payload,
        }
    }

    fn publish(&self, event_type: SessionEventType, payload: Value, durable: bool) -> SessionEvent {
        let _guard = self.lock();
        self.publish_locked(event_type, payload, durable)
    }

    fn publish_locked(
        &self,
        event_type: SessionEventType,
        payload: Value,
        durable: bool,
    ) -> SessionEvent {
        let event = self.build_locked(event_type, payload, durable);
        let _ = self.sender.send(event.clone());
        event
    }

    fn publish_steer(&self, control: &TurnControlSignal, prompt: &str) -> bool {
        let _guard = self.lock();
        if !control.steer(prompt.to_string()) {
            return false;
        }
        self.publish_locked(
            SessionEventType::TurnSteered,
            json!({ "prompt": prompt }),
            false,
        );
        true
    }

    fn publish_runtime_event(&self, event: ConversationEvent) {
        let (event_type, payload) = match event {
            ConversationEvent::AgentBegin { session_id: _ } => {
                (SessionEventType::AgentBegin, json!({}))
            }
            ConversationEvent::ToolUse {
                tool_use_id,
                tool_name,
                input,
            } => (
                SessionEventType::ToolUse,
                json!({
                    "toolUseId": tool_use_id,
                    "toolName": tool_name,
                    "input": input,
                }),
            ),
            ConversationEvent::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } => (
                SessionEventType::ToolResult,
                json!({
                    "toolUseId": tool_use_id,
                    "toolName": tool_name,
                    "output": output,
                    "isError": is_error,
                }),
            ),
            ConversationEvent::AgentEnd {
                session_id: _,
                interrupted,
                output,
            } => {
                let mut payload = json!({ "interrupted": interrupted });
                if let Some(output) = output {
                    payload["output"] = Value::String(output);
                }
                (SessionEventType::AgentEnd, payload)
            }
        };
        self.publish(event_type, payload, false);
    }
}

impl SessionRouter {
    /// Create a router backed by the supplied durable session store.
    ///
    /// Interrupted invocations left by an earlier process are moved to the
    /// uncertain state so clients can reconcile them against their correlation key.
    pub fn new(store: SessionStore, runner: Arc<dyn SessionTurnRunner>) -> SdkResult<Self> {
        let sdk_root = store.sessions_dir().join("sdk");
        let reconciliation = InvocationReconciliationStore::new(sdk_root.join("reconciliation"));
        let terminal_scopes = TerminalScopeStore::new(sdk_root.join("terminal-scopes"));
        reconciliation.reconcile_unfinished()?;
        recover_terminal_scopes(&reconciliation, &terminal_scopes)?;
        recover_pending_terminal_outcomes(&reconciliation, &terminal_scopes)?;
        Ok(Self {
            inner: Arc::new(SessionRouterInner {
                store,
                runner,
                reconciliation,
                terminal_scopes,
                reconciliation_lock: Mutex::new(()),
                active: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Create and persist an empty session, optionally assigning its model and correlation scope.
    pub fn create_session(
        &self,
        model: Option<&str>,
        correlation_key: Option<&str>,
    ) -> SdkResult<Session> {
        validate_optional_correlation_key(correlation_key)?;
        let model = model.map(str::trim).filter(|value| !value.is_empty());
        if model.is_some_and(|value| value.len() > 256) {
            return Err(SdkError::invalid_input("model name exceeds 256 bytes"));
        }

        let mut session =
            Session::new().with_workspace_root(self.inner.store.workspace_root().to_path_buf());
        session.model = model.map(str::to_string);
        let handle = self.inner.store.create_handle(&session.session_id);
        session = session.with_persistence_path(handle.path.clone());
        session
            .save_to_path(&handle.path)
            .map_err(|error| SdkError::internal(format!("could not persist session: {error}")))?;
        if let Some(key) = correlation_key {
            let _reconciliation_guard = self
                .inner
                .reconciliation_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.inner.terminal_scopes.set(key, &session.session_id)?;
        }
        Ok(session)
    }

    /// Load a session by its durable ID, rejecting aliases and path-like IDs.
    pub fn get_session(&self, session_id: &str) -> SdkResult<Session> {
        validate_session_id(session_id)?;
        let path = self
            .inner
            .store
            .resolve_managed_path(session_id)
            .map_err(|error| SdkError::not_found(error.to_string()))?;
        let session = Session::load_from_path(&path)
            .map_err(|error| SdkError::internal(format!("could not load session: {error}")))?;
        if session.session_id != session_id {
            return Err(SdkError::not_found(format!(
                "session `{session_id}` does not match the requested ID"
            )));
        }
        Ok(session)
    }

    /// Resolve the most recently recorded session for a correlation key.
    pub fn session_for_correlation_key(&self, correlation_key: &str) -> SdkResult<Option<String>> {
        validate_correlation_key(correlation_key)?;
        let _reconciliation_guard = self
            .inner
            .reconciliation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recover_pending_terminal_outcomes(&self.inner.reconciliation, &self.inner.terminal_scopes)?;
        let active_matches = {
            let active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            active
                .iter()
                .filter(|(_, invocation)| {
                    invocation.correlation_key.as_deref() == Some(correlation_key)
                })
                .map(|(session_id, invocation)| {
                    (session_id.clone(), invocation.invocation_id.clone())
                })
                .collect::<Vec<_>>()
        };
        if active_matches.len() > 1 {
            return Err(SdkError::conflict(format!(
                "correlation key '{correlation_key}' has multiple active sessions"
            )));
        }

        let has_unfinished_invocation = self
            .inner
            .reconciliation
            .list_by_correlation_key(correlation_key)?
            .iter()
            .any(|record| {
                record.status != InvocationStatus::Terminal
                    && active_matches
                        .first()
                        .is_none_or(|(_, active_id)| record.invocation_id != *active_id)
            });
        if has_unfinished_invocation {
            return Err(SdkError::unavailable(format!(
                "correlation key '{correlation_key}' has an invocation awaiting reconciliation"
            )));
        }
        if let Some((session_id, _)) = active_matches.first() {
            return Ok(Some(session_id.clone()));
        }
        let scope = self.inner.terminal_scopes.get(correlation_key)?;
        Ok(scope.map(|scope| scope.session_id))
    }

    /// Start a prompt and return immediately with an ordered event receiver.
    pub fn prompt(
        &self,
        session_id: &str,
        prompt: impl Into<String>,
        correlation_key: Option<&str>,
    ) -> SdkResult<SessionInvocation> {
        let prompt = prompt.into();
        validate_session_id(session_id)?;
        validate_prompt(&prompt)?;
        validate_optional_correlation_key(correlation_key)?;

        let mut active = self
            .inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.contains_key(session_id) {
            return Err(SdkError::conflict(format!(
                "session `{session_id}` already has an active invocation"
            )));
        }
        // Load after reserving the session slot. Loading before this lock lets a
        // waiting prompt retain a stale snapshot and overwrite the prior turn.
        let session = self.get_session(session_id)?;

        let invocation_id = Uuid::new_v4().to_string();
        let created_at_ms = now_ms();
        let record = InvocationRecord {
            invocation_id: invocation_id.clone(),
            session_id: session_id.to_string(),
            correlation_key: correlation_key.map(str::to_string),
            status: InvocationStatus::Accepted,
            accepted_at_ms: created_at_ms,
            updated_at_ms: created_at_ms,
            outcome: None,
            uncertainty_reason: None,
            terminal_event: None,
            terminal_scope_revision: None,
        };
        self.inner.reconciliation.insert(record)?;

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let publisher = Arc::new(SessionEventPublisher::new(
            session_id.to_string(),
            invocation_id.clone(),
            event_tx,
        ));
        let control = TurnControlSignal::new();
        let (done_tx, done_rx) = watch::channel(false);
        active.insert(
            session_id.to_string(),
            ActiveInvocation {
                invocation_id: invocation_id.clone(),
                correlation_key: correlation_key.map(str::to_string),
                control: control.clone(),
                event_publisher: publisher.clone(),
                done: done_rx,
            },
        );
        drop(active);

        publisher.publish(
            SessionEventType::TurnAccepted,
            json!({ "correlationKey": correlation_key }),
            false,
        );

        let inner = Arc::clone(&self.inner);
        let worker_session_id = session_id.to_string();
        let worker_invocation_id = invocation_id.clone();
        let worker_publisher = publisher.clone();
        let worker_control = control.clone();
        let worker_correlation_key = correlation_key.map(str::to_string);
        let worker_session = session.clone();
        let worker_done_tx = done_tx.clone();
        let worker = thread::Builder::new()
            .name(format!("sdk-session-{}", &invocation_id[..8]))
            .spawn(move || {
                if let Err(error) = inner.reconciliation.mark_running(&worker_invocation_id) {
                    finish_uncertain(
                        &inner,
                        &worker_invocation_id,
                        &worker_control,
                        &worker_publisher,
                        &error.to_string(),
                    );
                    remove_active(&inner, &worker_session_id, &worker_invocation_id);
                    let _ = worker_done_tx.send(true);
                    return;
                }
                let event_publisher = worker_publisher.clone();
                let callback: Arc<dyn Fn(ConversationEvent) + Send + Sync> =
                    Arc::new(move |event| event_publisher.publish_runtime_event(event));
                let result = catch_unwind(AssertUnwindSafe(|| {
                    inner
                        .runner
                        .run(worker_session, prompt, worker_control.clone(), callback)
                }));
                match result {
                    Ok(run_result) => finish_invocation(
                        &inner,
                        &worker_session_id,
                        &worker_invocation_id,
                        worker_correlation_key.as_deref(),
                        &worker_control,
                        &worker_publisher,
                        run_result,
                    ),
                    Err(panic) => {
                        let reason = panic_message(panic);
                        finish_uncertain(
                            &inner,
                            &worker_invocation_id,
                            &worker_control,
                            &worker_publisher,
                            &reason,
                        );
                    }
                }
                remove_active(&inner, &worker_session_id, &worker_invocation_id);
                let _ = worker_done_tx.send(true);
            });

        if let Err(error) = worker {
            let message = format!("could not start session worker: {error}");
            finish_invocation(
                &self.inner,
                session_id,
                &invocation_id,
                correlation_key,
                &control,
                &publisher,
                SessionRunResult::Failed { session, message },
            );
            remove_active(&self.inner, session_id, &invocation_id);
            let _ = done_tx.send(true);
        }

        Ok(SessionInvocation {
            session_id: session_id.to_string(),
            invocation_id,
            events: SessionEventStream { receiver: event_rx },
        })
    }

    /// Queue a steering prompt for the active invocation of a session.
    pub fn steer(
        &self,
        session_id: &str,
        invocation_id: &str,
        override_prompt: impl Into<String>,
    ) -> SdkResult<()> {
        validate_session_id(session_id)?;
        validate_invocation_id(invocation_id)?;
        let prompt = override_prompt.into();
        validate_prompt(&prompt)?;
        let (control, publisher) = {
            let active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = active
                .get(session_id)
                .ok_or_else(|| SdkError::not_found("session has no active invocation"))?;
            if invocation.invocation_id != invocation_id {
                return Err(SdkError::not_found(format!(
                    "invocation `{invocation_id}` is not active for session `{session_id}`"
                )));
            }
            (
                invocation.control.clone(),
                Arc::clone(&invocation.event_publisher),
            )
        };
        if !publisher.publish_steer(&control, &prompt) {
            return Err(SdkError::conflict("invocation no longer accepts steering"));
        }
        Ok(())
    }

    /// Abort the current invocation, wait for its durable terminal outcome, then start a new prompt.
    pub async fn abort_and_prompt(
        &self,
        session_id: &str,
        prompt: impl Into<String>,
        correlation_key: Option<&str>,
    ) -> SdkResult<SessionInvocation> {
        validate_session_id(session_id)?;
        let prompt = prompt.into();
        validate_prompt(&prompt)?;
        validate_optional_correlation_key(correlation_key)?;

        let (mut done, inherited_correlation, previous_invocation_id) = {
            let active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match active.get(session_id) {
                Some(invocation) => {
                    invocation.control.abort();
                    (
                        Some(invocation.done.clone()),
                        invocation.correlation_key.clone(),
                        Some(invocation.invocation_id.clone()),
                    )
                }
                None => (None, None, None),
            }
        };
        if let Some(receiver) = done.as_mut() {
            while !*receiver.borrow() {
                receiver.changed().await.map_err(|_| {
                    SdkError::unavailable("session invocation stopped unexpectedly")
                })?;
            }
        }

        if let Some(invocation_id) = previous_invocation_id {
            let record = self
                .inner
                .reconciliation
                .get(&invocation_id)?
                .ok_or_else(|| SdkError::not_found("aborted invocation record was not found"))?;
            if record.status != InvocationStatus::Terminal {
                return Err(SdkError::unavailable(format!(
                    "invocation `{invocation_id}` has no durable terminal outcome"
                )));
            }
        }

        let effective_correlation = correlation_key
            .map(str::to_string)
            .or(inherited_correlation);
        self.prompt(session_id, prompt, effective_correlation.as_deref())
    }

    /// List persisted sessions using an opaque offset cursor.
    pub fn list(&self, cursor: Option<&str>, limit: Option<usize>) -> SdkResult<SessionPage> {
        let offset = match cursor {
            Some(value) => value.parse::<usize>().map_err(|_| {
                SdkError::invalid_input("cursor must be a non-negative decimal offset")
            })?,
            None => 0,
        };
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(SdkError::invalid_input(format!(
                "limit must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        let sessions = self
            .inner
            .store
            .list_sessions()
            .map_err(|error| SdkError::internal(format!("could not list sessions: {error}")))?;
        let total = sessions.len();
        let summaries = sessions
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|session| SessionSummary {
                session_id: session.id,
                created_at_ms: session.created_at_ms,
                updated_at_ms: session.updated_at_ms,
                message_count: session.message_count,
            })
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(summaries.len());
        let next_cursor = (next_offset < total).then(|| next_offset.to_string());
        Ok(SessionPage {
            sessions: summaries,
            next_cursor,
        })
    }

    /// Return a durable invocation record for recovery after a disconnected client.
    pub fn reconcile(&self, invocation_id: &str) -> SdkResult<InvocationRecord> {
        validate_invocation_id(invocation_id)?;
        let _reconciliation_guard = self
            .inner
            .reconciliation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recover_pending_terminal_outcomes(&self.inner.reconciliation, &self.inner.terminal_scopes)?;
        self.inner
            .reconciliation
            .get(invocation_id)?
            .ok_or_else(|| {
                SdkError::not_found(format!("invocation `{invocation_id}` was not found"))
            })
    }

    /// Return all invocation outcomes associated with a correlation key in stable ID order.
    pub fn reconcile_by_correlation_key(
        &self,
        correlation_key: &str,
    ) -> SdkResult<Vec<InvocationRecord>> {
        validate_correlation_key(correlation_key)?;
        let _reconciliation_guard = self
            .inner
            .reconciliation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recover_pending_terminal_outcomes(&self.inner.reconciliation, &self.inner.terminal_scopes)?;
        self.inner
            .reconciliation
            .list_by_correlation_key(correlation_key)
    }

    /// Dispatch a broker request for `turn.create`, `turn.prompt`, `turn.steer`,
    /// `turn.abort_and_prompt`, or `turn.list`.
    pub async fn dispatch(&self, request: SdkFrame) -> SdkResult<SessionDispatch> {
        if request.frame_type()? != FrameType::BrokerRequest {
            return Err(SdkError::invalid_input(
                "session router requires a broker_request frame",
            ));
        }
        let request_id = request.id()?.ok_or_else(|| {
            SdkError::invalid_input("request frame is missing its correlation ID")
        })?;
        let operation = request
            .operation()
            .ok_or_else(|| SdkError::invalid_input("request frame is missing its operation"))?;
        let input = request.input().unwrap_or_else(|| json!({}));
        let object = input
            .as_object()
            .ok_or_else(|| SdkError::invalid_input("operation input must be a JSON object"))?;

        match operation.as_str() {
            "turn.create" => {
                let correlation_key = optional_string(object, "correlationKey")?;
                let model = optional_string(object, "model")?;
                let initial_prompt = optional_prompt(object, "input")?;
                if let Some(prompt) = initial_prompt.as_deref() {
                    validate_prompt(prompt)?;
                }
                let session = self.create_session(model.as_deref(), correlation_key.as_deref())?;
                if let Some(prompt) = initial_prompt {
                    let invocation =
                        self.prompt(&session.session_id, prompt, correlation_key.as_deref())?;
                    let response = SdkFrame::broker_response_ok(
                        request_id,
                        json!({
                            "sessionId": session.session_id,
                            "invocationId": invocation.invocation_id,
                        }),
                    );
                    Ok(SessionDispatch {
                        response,
                        events: Some(invocation.events),
                    })
                } else {
                    Ok(SessionDispatch {
                        response: SdkFrame::broker_response_ok(
                            request_id,
                            json!({ "sessionId": session.session_id }),
                        ),
                        events: None,
                    })
                }
            }
            "turn.prompt" => {
                let session_id = self.resolve_session_id(object)?;
                let prompt = required_turn_prompt(object)?;
                let correlation_key = optional_string(object, "correlationKey")?;
                let invocation = self.prompt(&session_id, prompt, correlation_key.as_deref())?;
                Ok(SessionDispatch {
                    response: SdkFrame::broker_response_ok(
                        request_id,
                        invocation_result(&invocation),
                    ),
                    events: Some(invocation.events),
                })
            }
            "turn.steer" => {
                let session_id = self.resolve_session_id(object)?;
                let invocation_id = required_string(object, "invocationId")?;
                let prompt = match object.get("override") {
                    Some(value) => prompt_value(value)?,
                    None => required_turn_prompt(object)?,
                };
                self.steer(&session_id, &invocation_id, prompt)?;
                Ok(SessionDispatch {
                    response: SdkFrame::broker_response_ok(request_id, json!({ "accepted": true })),
                    events: None,
                })
            }
            "turn.abort_and_prompt" => {
                let session_id = self.resolve_session_id(object)?;
                let prompt = required_turn_prompt(object)?;
                let correlation_key = optional_string(object, "correlationKey")?;
                let invocation = self
                    .abort_and_prompt(&session_id, prompt, correlation_key.as_deref())
                    .await?;
                Ok(SessionDispatch {
                    response: SdkFrame::broker_response_ok(
                        request_id,
                        invocation_result(&invocation),
                    ),
                    events: Some(invocation.events),
                })
            }
            "turn.list" => {
                let cursor = optional_string(object, "cursor")?;
                let limit = optional_usize(object, "limit")?;
                let page = self.list(cursor.as_deref(), limit)?;
                Ok(SessionDispatch {
                    response: SdkFrame::broker_response_ok(
                        request_id,
                        serde_json::to_value(page).map_err(|error| {
                            SdkError::internal(format!("could not encode session page: {error}"))
                        })?,
                    ),
                    events: None,
                })
            }
            _ => Err(SdkError::unknown_operation(format!(
                "unknown session operation `{operation}`"
            ))),
        }
    }

    fn resolve_session_id(&self, input: &Map<String, Value>) -> SdkResult<String> {
        if let Some(value) = input.get("sessionId") {
            let session_id = value
                .as_str()
                .ok_or_else(|| SdkError::invalid_input("field `sessionId` must be a string"))?;
            validate_session_id(session_id)?;
            return Ok(session_id.to_string());
        }
        if let Some(value) = input.get("correlationKey") {
            let correlation_key = value.as_str().ok_or_else(|| {
                SdkError::invalid_input("field `correlationKey` must be a string")
            })?;
            return self
                .session_for_correlation_key(correlation_key)?
                .ok_or_else(|| {
                    SdkError::not_found("no session is associated with this correlation key")
                });
        }
        Err(SdkError::invalid_input(
            "operation requires `sessionId` or a known `correlationKey`",
        ))
    }
}

fn invocation_result(invocation: &SessionInvocation) -> Value {
    json!({
        "sessionId": invocation.session_id,
        "invocationId": invocation.invocation_id,
    })
}

fn finish_invocation(
    inner: &SessionRouterInner,
    session_id: &str,
    invocation_id: &str,
    correlation_key: Option<&str>,
    control: &TurnControlSignal,
    publisher: &SessionEventPublisher,
    result: SessionRunResult,
) {
    let _reconciliation_guard = inner
        .reconciliation_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _publication_guard = publisher.lock();
    control.close();

    let (outcome, message_count, status, error_message) = match result {
        SessionRunResult::Completed(session) => (
            InvocationOutcome::Completed {
                message_count: session.messages.len(),
            },
            session.messages.len(),
            "completed",
            None,
        ),
        SessionRunResult::Aborted(session) => (
            InvocationOutcome::Aborted {
                message_count: session.messages.len(),
            },
            session.messages.len(),
            "aborted",
            None,
        ),
        SessionRunResult::Failed { session, message } => (
            InvocationOutcome::Failed {
                message: message.clone(),
                message_count: session.messages.len(),
            },
            session.messages.len(),
            "failed",
            Some(message),
        ),
    };
    let mut payload = json!({ "status": status, "messageCount": message_count });
    if let Some(error) = error_message {
        payload["error"] = Value::String(error);
    }
    let terminal_event = publisher.build_locked(SessionEventType::TurnEnded, payload, true);
    let stored_event = match serde_json::to_value(&terminal_event) {
        Ok(event) => event,
        Err(error) => {
            let _ = inner
                .reconciliation
                .mark_uncertain(invocation_id, error.to_string());
            return;
        }
    };
    if let Err(error) = inner.reconciliation.record_pending_terminal(
        invocation_id,
        outcome.clone(),
        stored_event.clone(),
    ) {
        let _ = inner
            .reconciliation
            .mark_uncertain(invocation_id, error.to_string());
        return;
    }
    let terminal_scope_revision = match correlation_key {
        Some(key) => match inner.terminal_scopes.set(key, session_id) {
            Ok(scope) => Some(scope.revision),
            Err(error) => {
                let _ = inner
                    .reconciliation
                    .mark_uncertain(invocation_id, error.to_string());
                return;
            }
        },
        None => None,
    };
    match inner.reconciliation.record_terminal_with_scope_revision(
        invocation_id,
        outcome,
        Some(stored_event),
        terminal_scope_revision,
    ) {
        Ok(()) => {
            let _ = publisher.sender.send(terminal_event);
        }
        Err(error) => {
            let _ = inner
                .reconciliation
                .mark_uncertain(invocation_id, error.to_string());
        }
    }
}

fn recover_terminal_scopes(
    reconciliation: &InvocationReconciliationStore,
    terminal_scopes: &TerminalScopeStore,
) -> SdkResult<()> {
    let mut latest_by_key = BTreeMap::<String, (u64, String)>::new();
    for record in reconciliation.list()? {
        if record.status != InvocationStatus::Terminal {
            continue;
        }
        let Some(key) = record.correlation_key.as_ref() else {
            continue;
        };
        let Some(revision) = record.terminal_scope_revision else {
            continue;
        };
        match latest_by_key.get(key) {
            Some((current_revision, current_session)) if *current_revision == revision => {
                if current_session != &record.session_id {
                    return Err(SdkError::internal(format!(
                        "terminal scope revision {revision} conflicts for key '{key}'"
                    )));
                }
            }
            Some((current_revision, _)) if *current_revision > revision => {}
            _ => {
                latest_by_key.insert(key.clone(), (revision, record.session_id));
            }
        }
    }
    for (key, (revision, session_id)) in latest_by_key {
        validate_correlation_key(&key)?;
        terminal_scopes.restore_if_newer(&key, &session_id, revision)?;
    }
    Ok(())
}

fn recover_pending_terminal_outcomes(
    reconciliation: &InvocationReconciliationStore,
    terminal_scopes: &TerminalScopeStore,
) -> SdkResult<()> {
    let records = reconciliation.list()?;
    let mut latest_terminal_update = BTreeMap::<String, u64>::new();
    let mut original_scope_update = BTreeMap::<String, Option<u64>>::new();
    let mut pending = Vec::new();
    for record in records {
        if record.status == InvocationStatus::Terminal {
            if let Some(key) = record.correlation_key.as_ref() {
                latest_terminal_update
                    .entry(key.clone())
                    .and_modify(|updated_at| *updated_at = (*updated_at).max(record.updated_at_ms))
                    .or_insert(record.updated_at_ms);
            }
        } else if record.outcome.is_some() && record.terminal_event.is_some() {
            pending.push(record);
        }
    }
    pending.sort_by(|left, right| {
        left.updated_at_ms
            .cmp(&right.updated_at_ms)
            .then_with(|| left.invocation_id.cmp(&right.invocation_id))
    });

    for record in pending {
        let scope_revision = match record.correlation_key.as_deref() {
            Some(key) => {
                let scope_updated_at = if let Some(updated_at) = original_scope_update.get(key) {
                    *updated_at
                } else {
                    let updated_at = terminal_scopes.get(key)?.map(|scope| scope.updated_at_ms);
                    original_scope_update.insert(key.to_string(), updated_at);
                    updated_at
                };
                let newer_terminal = latest_terminal_update
                    .get(key)
                    .is_some_and(|updated_at| *updated_at >= record.updated_at_ms);
                let newer_scope =
                    scope_updated_at.is_some_and(|updated_at| updated_at >= record.updated_at_ms);
                if newer_terminal || newer_scope {
                    None
                } else {
                    Some(terminal_scopes.set(key, &record.session_id)?.revision)
                }
            }
            None => None,
        };
        reconciliation.record_terminal_with_scope_revision(
            &record.invocation_id,
            record
                .outcome
                .ok_or_else(|| SdkError::internal("pending terminal outcome is missing"))?,
            Some(
                record
                    .terminal_event
                    .ok_or_else(|| SdkError::internal("pending terminal event is missing"))?,
            ),
            scope_revision,
        )?;
    }
    Ok(())
}

fn finish_uncertain(
    inner: &SessionRouterInner,
    invocation_id: &str,
    control: &TurnControlSignal,
    publisher: &SessionEventPublisher,
    reason: &str,
) {
    let _publication_guard = publisher.lock();
    control.close();
    let _ = inner
        .reconciliation
        .mark_uncertain(invocation_id, reason.to_string());
}

fn remove_active(inner: &SessionRouterInner, session_id: &str, invocation_id: &str) {
    let mut active = inner
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if active
        .get(session_id)
        .is_some_and(|value| value.invocation_id == invocation_id)
    {
        active.remove(session_id);
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            panic
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
        })
        .unwrap_or_else(|| "session runner panicked".to_string())
}

fn validate_session_id(session_id: &str) -> SdkResult<()> {
    let is_valid = !session_id.is_empty()
        && session_id.len() <= MAX_SESSION_ID_BYTES
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if is_valid {
        Ok(())
    } else {
        Err(SdkError::invalid_input("sessionId is malformed"))
    }
}

fn validate_invocation_id(invocation_id: &str) -> SdkResult<()> {
    let is_uuid = Uuid::parse_str(invocation_id).is_ok();
    if is_uuid {
        Ok(())
    } else {
        Err(SdkError::invalid_input("invocationId must be a UUID"))
    }
}

fn validate_correlation_key(correlation_key: &str) -> SdkResult<()> {
    if correlation_key.len() <= MAX_CORRELATION_KEY_BYTES {
        Ok(())
    } else {
        Err(SdkError::invalid_input("correlationKey exceeds 512 bytes"))
    }
}

fn validate_optional_correlation_key(correlation_key: Option<&str>) -> SdkResult<()> {
    correlation_key.map_or(Ok(()), validate_correlation_key)
}

fn validate_prompt(prompt: &str) -> SdkResult<()> {
    if !prompt.trim().is_empty() && prompt.len() <= 1_000_000 {
        Ok(())
    } else {
        Err(SdkError::invalid_input(
            "prompt must be non-empty and no larger than 1 MiB",
        ))
    }
}

fn required_string(object: &Map<String, Value>, key: &str) -> SdkResult<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| SdkError::invalid_input(format!("missing string field `{key}`")))
}

fn optional_prompt(object: &Map<String, Value>, key: &str) -> SdkResult<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => prompt_value(value).map(Some),
    }
}

fn required_turn_prompt(object: &Map<String, Value>) -> SdkResult<String> {
    for key in ["prompt", "request", "input"] {
        if let Some(value) = object.get(key) {
            return prompt_value(value);
        }
    }
    Err(SdkError::invalid_input(
        "operation requires a string `prompt` or `request` input",
    ))
}

fn prompt_value(value: &Value) -> SdkResult<String> {
    match value {
        Value::String(prompt) => Ok(prompt.clone()),
        Value::Object(object) => {
            if let Some(messages) = object.get("messages").and_then(Value::as_array) {
                if let Some(user_message) = messages.iter().rev().find(|message| {
                    message
                        .get("role")
                        .and_then(Value::as_str)
                        .is_some_and(|role| role == "user")
                }) {
                    if let Some(content) = user_message.get("content") {
                        return prompt_value(content);
                    }
                }
            }
            for key in ["prompt", "input", "text", "content"] {
                if let Some(value) = object.get(key) {
                    return prompt_value(value);
                }
            }
            Err(SdkError::invalid_input(
                "request object must contain prompt text or a user message",
            ))
        }
        Value::Array(blocks) => {
            let mut text = Vec::new();
            for block in blocks {
                match block {
                    Value::String(value) => text.push(value.clone()),
                    Value::Object(object) => {
                        if let Some(value) = object.get("text").and_then(Value::as_str) {
                            text.push(value.to_string());
                        } else if let Some(value) = object.get("content") {
                            text.push(prompt_value(value)?);
                        }
                    }
                    _ => {}
                }
            }
            if text.is_empty() {
                Err(SdkError::invalid_input(
                    "request content has no text blocks",
                ))
            } else {
                Ok(text.join("\n"))
            }
        }
        _ => Err(SdkError::invalid_input(
            "prompt/request content must contain text",
        )),
    }
}

fn optional_string(object: &Map<String, Value>, key: &str) -> SdkResult<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(SdkError::invalid_input(format!(
            "field `{key}` must be a string"
        ))),
    }
}

fn optional_usize(object: &Map<String, Value>, key: &str) -> SdkResult<Option<usize>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|number| usize::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| SdkError::invalid_input(format!("field `{key}` must be positive"))),
        Some(_) => Err(SdkError::invalid_input(format!(
            "field `{key}` must be an integer"
        ))),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u128::from(u64::MAX)) as u64
        })
}
