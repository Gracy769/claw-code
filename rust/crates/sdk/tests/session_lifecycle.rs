use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use runtime::{ConversationEvent, Session, SessionStore, TurnControlSignal};
use sdk::error::SdkErrorCode as ErrorCode;
use sdk::session::reconciliation::{InvocationOutcome, InvocationRecord, InvocationStatus};
use sdk::{
    RuntimeSessionRunner, SdkFrame as Frame, SessionEvent, SessionEventType, SessionRouter,
    SessionRunResult, SessionTurnRunner,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use uuid::Uuid;

struct EchoRunner;

impl SessionTurnRunner for EchoRunner {
    fn run(
        &self,
        mut session: Session,
        prompt: String,
        _control: TurnControlSignal,
        callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        let session_id = session.session_id.clone();
        callback(ConversationEvent::AgentBegin {
            session_id: session_id.clone(),
        });
        if let Err(error) = session.push_user_text(prompt) {
            return SessionRunResult::Failed {
                session,
                message: error.to_string(),
            };
        }
        callback(ConversationEvent::ToolUse {
            tool_use_id: "tool-1".to_string(),
            tool_name: "inspect".to_string(),
            input: "{}".to_string(),
        });
        callback(ConversationEvent::ToolResult {
            tool_use_id: "tool-1".to_string(),
            tool_name: "inspect".to_string(),
            output: "done".to_string(),
            is_error: false,
        });
        callback(ConversationEvent::AgentEnd {
            session_id,
            interrupted: false,
            output: None,
        });
        SessionRunResult::Completed(session)
    }
}

struct FailingRunner;

impl SessionTurnRunner for FailingRunner {
    fn run(
        &self,
        session: Session,
        _prompt: String,
        _control: TurnControlSignal,
        _callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        SessionRunResult::Failed {
            session,
            message: "upstream failed".to_string(),
        }
    }
}

struct PanicRunner;

impl SessionTurnRunner for PanicRunner {
    fn run(
        &self,
        _session: Session,
        _prompt: String,
        _control: TurnControlSignal,
        _callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        panic!("injected runner panic");
    }
}

struct GateRunner {
    started: Mutex<Option<mpsc::SyncSender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl SessionTurnRunner for GateRunner {
    fn run(
        &self,
        mut session: Session,
        prompt: String,
        control: TurnControlSignal,
        callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        let session_id = session.session_id.clone();
        callback(ConversationEvent::AgentBegin {
            session_id: session_id.clone(),
        });
        if let Some(started) = self
            .started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = started.send(());
        }
        let _ = self
            .release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        if let Err(error) = session.push_user_text(prompt) {
            return SessionRunResult::Failed {
                session,
                message: error.to_string(),
            };
        }
        while let Some(steer) = control.take_steer() {
            if let Err(error) = session.push_user_text(steer) {
                return SessionRunResult::Failed {
                    session,
                    message: error.to_string(),
                };
            }
        }
        let _ = control.finish_or_take_steer();
        callback(ConversationEvent::AgentEnd {
            session_id,
            interrupted: false,
            output: None,
        });
        SessionRunResult::Completed(session)
    }
}

struct AbortRunner {
    calls: AtomicUsize,
}

impl SessionTurnRunner for AbortRunner {
    fn run(
        &self,
        mut session: Session,
        prompt: String,
        control: TurnControlSignal,
        callback: Arc<dyn Fn(ConversationEvent) + Send + Sync>,
    ) -> SessionRunResult {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let session_id = session.session_id.clone();
        callback(ConversationEvent::AgentBegin {
            session_id: session_id.clone(),
        });
        if call == 0 {
            while !control.is_aborted() {
                thread::sleep(Duration::from_millis(1));
            }
            callback(ConversationEvent::AgentEnd {
                session_id,
                interrupted: true,
                output: None,
            });
            return SessionRunResult::Aborted(session);
        }
        if let Err(error) = session.push_user_text(prompt) {
            return SessionRunResult::Failed {
                session,
                message: error.to_string(),
            };
        }
        callback(ConversationEvent::AgentEnd {
            session_id,
            interrupted: false,
            output: None,
        });
        SessionRunResult::Completed(session)
    }
}

fn setup() -> (TempDir, SessionStore, SessionRouter) {
    setup_with(Arc::new(EchoRunner))
}

fn setup_with(runner: Arc<dyn SessionTurnRunner>) -> (TempDir, SessionStore, SessionRouter) {
    let temp = TempDir::new().expect("temporary directory");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace directory");
    let store =
        SessionStore::from_data_dir(temp.path().join("data"), &workspace).expect("session store");
    let router = SessionRouter::new(store.clone(), runner).expect("session router");
    (temp, store, router)
}

fn broker_request(operation: &str, input: Value) -> Frame {
    Frame::broker_request(Uuid::new_v4(), operation.to_string(), input)
}

async fn collect_events(mut invocation: sdk::SessionInvocation) -> Vec<SessionEvent> {
    let mut events = Vec::new();
    while let Some(event) = invocation.events.recv().await {
        events.push(event);
    }
    events
}

async fn collect_stream(mut stream: sdk::SessionEventStream) -> Vec<SessionEvent> {
    let mut events = Vec::new();
    while let Some(event) = stream.recv().await {
        events.push(event);
    }
    events
}

fn assert_invalid_input(error: sdk::SdkError) {
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[test]
fn create_persists_session_for_durable_lookup() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let restored = router
        .get_session(&session.session_id)
        .expect("stored session should resolve by ID");
    assert_eq!(restored.session_id, session.session_id);
}

#[test]
fn create_persists_model_metadata() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(Some("claude-test"), None)
        .expect("session should persist");
    let restored = router
        .get_session(&session.session_id)
        .expect("stored session should reload");
    assert_eq!(restored.model.as_deref(), Some("claude-test"));
}

#[test]
fn create_trims_model_name() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(Some("  model-x  "), None)
        .expect("session should persist");
    assert_eq!(session.model.as_deref(), Some("model-x"));
}

#[test]
fn create_maps_correlation_key_to_session() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, Some("discord:conversation:7"))
        .expect("session should persist");
    assert_eq!(
        router
            .session_for_correlation_key("discord:conversation:7")
            .expect("scope read"),
        Some(session.session_id)
    );
}

#[test]
fn create_without_correlation_key_does_not_create_scope() {
    let (_temp, _store, router) = setup();
    router
        .create_session(None, None)
        .expect("session should persist");
    assert_eq!(
        router
            .session_for_correlation_key("missing")
            .expect("scope read"),
        None
    );
}

#[test]
fn create_without_filesystem_side_effects_until_first_session() {
    let temp = TempDir::new().expect("temporary directory");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace directory");
    let store =
        SessionStore::from_data_dir(temp.path().join("data"), &workspace).expect("session store");
    let sdk_root = store.sessions_dir().join("sdk");
    let router = SessionRouter::new(store, Arc::new(EchoRunner)).expect("router");
    assert!(!sdk_root.exists());
    drop(router);
    assert!(!sdk_root.exists());
}

#[test]
fn session_lookup_returns_not_found_for_unknown_id() {
    let (_temp, _store, router) = setup();
    let error = router.get_session("session-0000000000000-99").unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn prompt_publishes_lifecycle_events_in_order() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "hello", None)
        .expect("prompt should start");
    let events = collect_events(invocation).await;
    let kinds = events
        .iter()
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            SessionEventType::TurnAccepted,
            SessionEventType::AgentBegin,
            SessionEventType::ToolUse,
            SessionEventType::ToolResult,
            SessionEventType::AgentEnd,
            SessionEventType::TurnEnded,
        ]
    );
}

#[tokio::test]
async fn prompt_assigns_monotonic_event_sequences() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let events = collect_events(
        router
            .prompt(&session.session_id, "hello", None)
            .expect("prompt should start"),
    )
    .await;
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
    assert!(events
        .windows(2)
        .all(|pair| pair[0].timestamp_ms <= pair[1].timestamp_ms));
}

#[tokio::test]
async fn prompt_persists_durable_terminal_event() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "hello", Some("corr-terminal"))
        .expect("prompt should start");
    let invocation_id = invocation.invocation_id.clone();
    let events = collect_events(invocation).await;
    let terminal = events.last().expect("terminal event");
    assert_eq!(terminal.event_type, SessionEventType::TurnEnded);
    assert!(terminal.durable);
    let record = router
        .reconcile(&invocation_id)
        .expect("terminal record should reconcile");
    assert_eq!(record.status, InvocationStatus::Terminal);
    assert_eq!(
        record.outcome,
        Some(InvocationOutcome::Completed { message_count: 1 })
    );
    assert_eq!(
        record.terminal_event,
        Some(serde_json::to_value(terminal).unwrap())
    );
}

#[tokio::test]
async fn prompt_updates_terminal_scope_when_turn_finishes() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "hello", Some("corr-finish"))
        .expect("prompt should start");
    collect_events(invocation).await;
    assert_eq!(
        router
            .session_for_correlation_key("corr-finish")
            .expect("scope read"),
        Some(session.session_id)
    );
}

#[tokio::test]
async fn prompt_rejects_second_active_invocation_for_session() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let first = router
        .prompt(&session.session_id, "first", None)
        .expect("first prompt should start");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("runner should start");
    let error = router
        .prompt(&session.session_id, "second", None)
        .expect_err("parallel invocation should be rejected");
    assert_eq!(error.code, ErrorCode::Conflict);
    release_tx.send(()).expect("release runner");
    collect_events(first).await;
}

#[tokio::test]
async fn steer_is_accepted_for_the_current_active_invocation() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "first", None)
        .expect("prompt should start");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("runner should start");
    router
        .steer(&session.session_id, &invocation.invocation_id, "redirect")
        .expect("active invocation should accept steering");
    release_tx.send(()).expect("release runner");
    let events = collect_events(invocation).await;
    assert!(events
        .iter()
        .any(|event| event.event_type == SessionEventType::TurnSteered));
    let session = router
        .get_session(&session.session_id)
        .expect("session should remain loadable");
    assert_eq!(session.messages.len(), 2);
}

#[test]
fn steer_rejects_unknown_invocation_id() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let error = router
        .steer(&session.session_id, &Uuid::new_v4().to_string(), "redirect")
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
}

#[test]
fn steer_rejects_malformed_invocation_id() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let error = router
        .steer(&session.session_id, "not-a-uuid", "redirect")
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn abort_and_prompt_persists_old_terminal_outcome_and_starts_new_turn() {
    let runner = Arc::new(AbortRunner {
        calls: AtomicUsize::new(0),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let previous = router
        .prompt(&session.session_id, "long-running", None)
        .expect("initial invocation");
    let previous_id = previous.invocation_id.clone();
    let next = router
        .abort_and_prompt(&session.session_id, "replacement", None)
        .await
        .expect("abort should finish before replacement starts");
    assert_ne!(next.invocation_id, previous_id);
    let old_events = collect_events(previous).await;
    assert_eq!(
        old_events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    assert_eq!(
        router.reconcile(&previous_id).unwrap().outcome,
        Some(InvocationOutcome::Aborted { message_count: 0 })
    );
    let new_events = collect_events(next).await;
    assert_eq!(
        new_events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    assert_eq!(
        router
            .reconcile(&new_events[0].invocation_id)
            .unwrap()
            .status,
        InvocationStatus::Terminal
    );
}

#[tokio::test]
async fn abort_and_prompt_without_active_turn_starts_prompt() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .abort_and_prompt(&session.session_id, "new prompt", None)
        .await
        .expect("new prompt should start");
    let events = collect_events(invocation).await;
    assert_eq!(events[0].event_type, SessionEventType::TurnAccepted);
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
}

#[tokio::test]
async fn runner_failure_is_a_terminal_failed_outcome() {
    let (_temp, _store, router) = setup_with(Arc::new(FailingRunner));
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "hello", None)
        .expect("prompt should start");
    let invocation_id = invocation.invocation_id.clone();
    let events = collect_events(invocation).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    assert_eq!(
        router.reconcile(&invocation_id).unwrap().outcome,
        Some(InvocationOutcome::Failed {
            message: "upstream failed".to_string(),
            message_count: 0,
        })
    );
}

#[tokio::test]
async fn runner_panic_marks_invocation_uncertain_for_recovery() {
    let (_temp, _store, router) = setup_with(Arc::new(PanicRunner));
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "hello", None)
        .expect("prompt should start");
    let invocation_id = invocation.invocation_id.clone();
    let events = collect_events(invocation).await;
    assert_eq!(events.len(), 1);
    let record = router
        .reconcile(&invocation_id)
        .expect("record should remain");
    assert_eq!(record.status, InvocationStatus::Uncertain);
    assert!(record
        .uncertainty_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("injected runner panic")));
}

#[test]
fn list_returns_empty_page_without_sessions() {
    let (_temp, _store, router) = setup();
    let page = router.list(None, None).expect("empty page");
    assert!(page.sessions.is_empty());
    assert_eq!(page.next_cursor, None);
}

#[test]
fn list_paginates_without_duplicate_sessions() {
    let (_temp, _store, router) = setup();
    let first = router.create_session(None, None).expect("first session");
    let second = router.create_session(None, None).expect("second session");
    let third = router.create_session(None, None).expect("third session");
    let first_page = router.list(None, Some(2)).expect("first page");
    assert_eq!(first_page.sessions.len(), 2);
    let cursor = first_page.next_cursor.expect("next cursor");
    let second_page = router.list(Some(&cursor), Some(2)).expect("second page");
    assert_eq!(second_page.sessions.len(), 1);
    assert_eq!(second_page.next_cursor, None);
    let mut ids = first_page
        .sessions
        .iter()
        .chain(second_page.sessions.iter())
        .map(|session| session.session_id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    let mut expected = vec![first.session_id, second.session_id, third.session_id];
    expected.sort();
    assert_eq!(ids, expected);
}

#[test]
fn list_rejects_zero_limit() {
    let (_temp, _store, router) = setup();
    assert_invalid_input(router.list(None, Some(0)).unwrap_err());
}

#[test]
fn list_rejects_limit_above_maximum() {
    let (_temp, _store, router) = setup();
    assert_invalid_input(router.list(None, Some(101)).unwrap_err());
}

#[test]
fn list_rejects_malformed_cursor() {
    let (_temp, _store, router) = setup();
    assert_invalid_input(router.list(Some("offset"), None).unwrap_err());
}

#[tokio::test]
async fn dispatch_turn_create_returns_durable_session_id() {
    let (_temp, _store, router) = setup();
    let dispatch = router
        .dispatch(broker_request(
            "turn.create",
            json!({ "model": "model-y", "correlationKey": "thread-1" }),
        ))
        .await
        .expect("create dispatch");
    assert!(dispatch.events.is_none());
    let session_id = dispatch.response.result().unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        router.get_session(&session_id).unwrap().model.as_deref(),
        Some("model-y")
    );
    assert_eq!(
        router.session_for_correlation_key("thread-1").unwrap(),
        Some(session_id)
    );
}

#[tokio::test]
async fn dispatch_turn_create_with_input_starts_stream() {
    let (_temp, _store, router) = setup();
    let dispatch = router
        .dispatch(broker_request("turn.create", json!({ "input": "start" })))
        .await
        .expect("create dispatch");
    assert!(dispatch.response.result().unwrap()["invocationId"].is_string());
    let mut stream = dispatch.events.expect("event stream");
    let first = stream.recv().await.expect("accepted event");
    assert_eq!(first.event_type, SessionEventType::TurnAccepted);
    while stream.recv().await.is_some() {}
}

#[tokio::test]
async fn dispatch_turn_create_accepts_object_prompt_input() {
    let (_temp, _store, router) = setup();
    let dispatch = router
        .dispatch(broker_request(
            "turn.create",
            json!({ "input": { "prompt": "start from object" } }),
        ))
        .await
        .expect("create dispatch");
    let events = collect_stream(dispatch.events.expect("event stream")).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
}

#[tokio::test]
async fn dispatch_turn_prompt_returns_events() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let dispatch = router
        .dispatch(broker_request(
            "turn.prompt",
            json!({ "sessionId": session.session_id, "prompt": "hello" }),
        ))
        .await
        .expect("prompt dispatch");
    assert!(dispatch.response.result().unwrap()["invocationId"].is_string());
    let events = collect_stream(dispatch.events.expect("event stream")).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
}

#[tokio::test]
async fn dispatch_turn_prompt_accepts_request_message_shape() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let dispatch = router
        .dispatch(broker_request(
            "turn.prompt",
            json!({
                "sessionId": session.session_id,
                "request": {
                    "messages": [
                        { "role": "assistant", "content": "prior context" },
                        { "role": "user", "content": "prompt from request" }
                    ]
                },
            }),
        ))
        .await
        .expect("request-shaped prompt should dispatch");
    let events = collect_stream(dispatch.events.expect("event stream")).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    let restored = router.get_session(&session.session_id).unwrap();
    assert!(restored.messages.iter().any(|message| {
        message.role == runtime::MessageRole::User
            && message.blocks.iter().any(|block| {
                matches!(block, runtime::ContentBlock::Text { text } if text == "prompt from request")
            })
    }));
}

#[tokio::test]
async fn dispatch_turn_prompt_resolves_terminal_scope() {
    let (_temp, _store, router) = setup();
    let session = router
        .create_session(None, Some("scope-prompt"))
        .expect("session should persist");
    let dispatch = router
        .dispatch(broker_request(
            "turn.prompt",
            json!({ "correlationKey": "scope-prompt", "prompt": "hello" }),
        ))
        .await
        .expect("scoped prompt");
    assert_eq!(
        dispatch.response.result().unwrap()["sessionId"],
        session.session_id
    );
    collect_stream(dispatch.events.expect("event stream")).await;
}

#[tokio::test]
async fn dispatch_turn_steer_accepts_override() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let invocation = router
        .prompt(&session.session_id, "initial", None)
        .expect("initial turn");
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let response = router
        .dispatch(broker_request(
            "turn.steer",
            json!({
                "sessionId": session.session_id,
                "invocationId": invocation.invocation_id,
                "override": { "prompt": "change direction" },
            }),
        ))
        .await
        .expect("steer dispatch");
    assert_eq!(response.response.result().unwrap()["accepted"], true);
    release_tx.send(()).unwrap();
    collect_events(invocation).await;
}

#[tokio::test]
async fn dispatch_turn_abort_and_prompt_returns_replacement_invocation() {
    let runner = Arc::new(AbortRunner {
        calls: AtomicUsize::new(0),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router
        .create_session(None, None)
        .expect("session should persist");
    let previous = router
        .prompt(&session.session_id, "first", None)
        .expect("initial invocation");
    let response = router
        .dispatch(broker_request(
            "turn.abort_and_prompt",
            json!({ "sessionId": session.session_id, "prompt": "replacement" }),
        ))
        .await
        .expect("abort and prompt dispatch");
    assert_ne!(
        response.response.result().unwrap()["invocationId"],
        previous.invocation_id
    );
    let old_events = collect_events(previous).await;
    assert_eq!(
        old_events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    let new_events = collect_stream(response.events.expect("replacement event stream")).await;
    assert_eq!(
        new_events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
}

#[tokio::test]
async fn dispatch_turn_list_serializes_pagination() {
    let (_temp, _store, router) = setup();
    router.create_session(None, None).unwrap();
    let response = router
        .dispatch(broker_request("turn.list", json!({ "limit": 1 })))
        .await
        .expect("list dispatch");
    let result = response.response.result().unwrap();
    assert_eq!(result["sessions"].as_array().unwrap().len(), 1);
    assert!(result["sessions"][0]["sessionId"].is_string());
}

#[tokio::test]
async fn dispatch_rejects_unknown_session_operation() {
    let (_temp, _store, router) = setup();
    let error = router
        .dispatch(broker_request("turn.unknown", json!({})))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::UnknownOperation);
}

#[tokio::test]
async fn dispatch_rejects_non_broker_frame() {
    let (_temp, _store, router) = setup();
    let error = router.dispatch(Frame::broker_hello()).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn dispatch_rejects_missing_request_id() {
    let (_temp, _store, router) = setup();
    let mut data = serde_json::Map::new();
    data.insert(
        "type".to_string(),
        Value::String("broker_request".to_string()),
    );
    data.insert(
        "operation".to_string(),
        Value::String("turn.list".to_string()),
    );
    data.insert("input".to_string(), json!({}));
    let error = router.dispatch(Frame::new(data)).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn dispatch_rejects_non_object_operation_input() {
    let (_temp, _store, router) = setup();
    let error = router
        .dispatch(broker_request("turn.list", json!("invalid")))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[test]
fn reconciliation_returns_not_found_for_unknown_invocation() {
    let (_temp, _store, router) = setup();
    let error = router.reconcile(&Uuid::new_v4().to_string()).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
}

#[test]
fn startup_recovers_running_invocations_as_uncertain() {
    let (_temp, store, router) = setup();
    let session = router
        .create_session(None, Some("recover-me"))
        .expect("session should persist");
    let invocation_id = Uuid::new_v4().to_string();
    let reconciliation = sdk::InvocationReconciliationStore::new(
        store.sessions_dir().join("sdk").join("reconciliation"),
    );
    reconciliation
        .insert(InvocationRecord {
            invocation_id: invocation_id.clone(),
            session_id: session.session_id,
            correlation_key: Some("recover-me".to_string()),
            status: InvocationStatus::Running,
            accepted_at_ms: 1,
            updated_at_ms: 1,
            outcome: None,
            uncertainty_reason: None,
            terminal_event: None,
            terminal_scope_revision: None,
        })
        .expect("running record");
    let restarted = SessionRouter::new(store, Arc::new(EchoRunner)).expect("new router");
    let record = restarted.reconcile(&invocation_id).unwrap();
    assert_eq!(record.status, InvocationStatus::Uncertain);
    let error = restarted
        .session_for_correlation_key("recover-me")
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Unavailable);
}

#[test]
fn terminal_scope_survives_router_restart() {
    let (temp, store, router) = setup();
    let session = router
        .create_session(None, Some("restart-scope"))
        .expect("session should persist");
    drop(router);
    let restarted = SessionRouter::new(store, Arc::new(EchoRunner)).expect("new router");
    assert_eq!(
        restarted
            .session_for_correlation_key("restart-scope")
            .unwrap(),
        Some(session.session_id)
    );
    assert!(temp.path().exists());
}

#[tokio::test]
async fn terminal_scope_write_failure_preserves_outcome_for_reconciliation() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, store, router) = setup_with(runner);
    let stale_session = router.create_session(None, Some("repair-key")).unwrap();
    let current_session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&current_session.session_id, "complete", Some("repair-key"))
        .unwrap();
    let invocation_id = invocation.invocation_id.clone();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let scope_root = store.sessions_dir().join("sdk").join("terminal-scopes");
    std::fs::remove_dir_all(&scope_root).unwrap();
    std::fs::write(&scope_root, "block the scope store").unwrap();
    release_tx.send(()).unwrap();

    let events = collect_events(invocation).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::AgentEnd
    );
    let reconciliation = sdk::InvocationReconciliationStore::new(
        store.sessions_dir().join("sdk").join("reconciliation"),
    );
    let pending = reconciliation
        .get(&invocation_id)
        .unwrap()
        .expect("pending result should be durable");
    assert_eq!(pending.status, InvocationStatus::Uncertain);
    assert_eq!(
        pending.outcome,
        Some(InvocationOutcome::Completed { message_count: 1 })
    );
    assert_eq!(
        pending.terminal_event.as_ref().unwrap()["eventType"],
        "turn_ended"
    );
    assert!(router.session_for_correlation_key("repair-key").is_err());

    std::fs::remove_file(&scope_root).unwrap();
    std::fs::create_dir_all(&scope_root).unwrap();
    drop(router);
    let restarted = SessionRouter::new(store, Arc::new(EchoRunner)).unwrap();
    let recovered = restarted.reconcile(&invocation_id).unwrap();
    assert_eq!(recovered.status, InvocationStatus::Terminal);
    assert_eq!(
        restarted.session_for_correlation_key("repair-key").unwrap(),
        Some(current_session.session_id.clone())
    );
    assert_ne!(stale_session.session_id, current_session.session_id);
}

#[test]
fn terminal_scope_keys_do_not_collide() {
    let (_temp, _store, router) = setup();
    let first = router
        .create_session(None, Some("key-one"))
        .expect("first session");
    let second = router
        .create_session(None, Some("key-two"))
        .expect("second session");
    assert_ne!(first.session_id, second.session_id);
    assert_eq!(
        router.session_for_correlation_key("key-one").unwrap(),
        Some(first.session_id)
    );
    assert_eq!(
        router.session_for_correlation_key("key-two").unwrap(),
        Some(second.session_id)
    );
}

#[tokio::test]
async fn correlation_lookup_rejects_multiple_active_sessions() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(2);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let first = router.create_session(None, Some("active-key")).unwrap();
    let second = router.create_session(None, Some("active-key")).unwrap();
    let first_invocation = router
        .prompt(&first.session_id, "first", Some("active-key"))
        .unwrap();
    let second_invocation = router
        .prompt(&second.session_id, "second", Some("active-key"))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    assert_eq!(
        router
            .session_for_correlation_key("active-key")
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    collect_events(first_invocation).await;
    collect_events(second_invocation).await;
}

#[tokio::test]
async fn correlation_lookup_does_not_bypass_an_older_uncertain_invocation() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, store, router) = setup_with(runner);
    let older_session = router.create_session(None, None).unwrap();
    let current_session = router.create_session(None, None).unwrap();
    let reconciliation = sdk::InvocationReconciliationStore::new(
        store.sessions_dir().join("sdk").join("reconciliation"),
    );
    reconciliation
        .insert(InvocationRecord {
            invocation_id: Uuid::new_v4().to_string(),
            session_id: older_session.session_id,
            correlation_key: Some("shared-key".to_string()),
            status: InvocationStatus::Uncertain,
            accepted_at_ms: 1,
            updated_at_ms: 2,
            outcome: None,
            uncertainty_reason: Some("recovered unfinished invocation".to_string()),
            terminal_event: None,
            terminal_scope_revision: None,
        })
        .unwrap();

    let invocation = router
        .prompt(&current_session.session_id, "current", Some("shared-key"))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        router
            .session_for_correlation_key("shared-key")
            .unwrap_err()
            .code,
        ErrorCode::Unavailable
    );

    release_tx.send(()).unwrap();
    collect_events(invocation).await;
}

#[tokio::test]
async fn session_router_clones_share_active_invocations() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let cloned = router.clone();
    let session = router.create_session(None, None).unwrap();
    let invocation = router.prompt(&session.session_id, "hello", None).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        cloned
            .prompt(&session.session_id, "parallel", None)
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    release_tx.send(()).unwrap();
    collect_events(invocation).await;
}

#[test]
fn reconciliation_terminal_outcome_retains_correlation_key() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&session.session_id, "hello", Some("corr-record"))
        .unwrap();
    let invocation_id = invocation.invocation_id.clone();
    drop(invocation);
    // The runner is intentionally asynchronous; its durable record is awaited by polling below.
    for _ in 0..200 {
        if router
            .reconcile(&invocation_id)
            .is_ok_and(|record| record.status == InvocationStatus::Terminal)
        {
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    let record = router.reconcile(&invocation_id).unwrap();
    assert_eq!(record.correlation_key.as_deref(), Some("corr-record"));
}

#[tokio::test]
async fn reconciliation_can_resume_all_invocations_for_a_correlation_key() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&session.session_id, "hello", Some("corr-history"))
        .unwrap();
    let invocation_id = invocation.invocation_id.clone();
    collect_events(invocation).await;
    let records = router
        .reconcile_by_correlation_key("corr-history")
        .expect("correlation reconciliation");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].invocation_id, invocation_id);
    assert_eq!(records[0].status, InvocationStatus::Terminal);
}

#[test]
fn create_rejects_model_longer_than_limit() {
    let (_temp, _store, router) = setup();
    let error = router
        .create_session(Some(&"m".repeat(257)), None)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

macro_rules! malformed_session_id_tests {
    ($($name:ident => $session_id:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                let (_temp, _store, router) = setup();
                assert_invalid_input(router.get_session($session_id).unwrap_err());
            }
        )+
    };
}

malformed_session_id_tests! {
    malformed_session_id_empty => "",
    malformed_session_id_dot => ".",
    malformed_session_id_dot_dot => "..",
    malformed_session_id_forward_slash => "folder/session",
    malformed_session_id_parent_traversal => "../outside",
    malformed_session_id_absolute_path => "/tmp/session",
    malformed_session_id_windows_path => "C:\\temp\\session",
    malformed_session_id_backslash => "folder\\session",
    malformed_session_id_space => "session id",
    malformed_session_id_unicode => "sëssion",
    malformed_session_id_nul => "session\0id",
    malformed_session_id_too_long => "s".repeat(129).as_str(),
}

macro_rules! malformed_invocation_id_tests {
    ($($name:ident => $invocation_id:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                let (_temp, _store, router) = setup();
                assert_invalid_input(router.reconcile($invocation_id).unwrap_err());
            }
        )+
    };
}

malformed_invocation_id_tests! {
    malformed_invocation_id_empty => "",
    malformed_invocation_id_path => "../invocation",
    malformed_invocation_id_short => "123",
    malformed_invocation_id_invalid_character => "00000000-0000-0000-0000-00000000000g",
    malformed_invocation_id_braces => "{00000000-0000-0000-0000-000000000000}",
    malformed_invocation_id_whitespace => " 00000000-0000-0000-0000-000000000000",
    malformed_invocation_id_extra_suffix => "00000000-0000-0000-0000-000000000000.json",
    malformed_invocation_id_too_many_sections => "00000000-0000-0000-0000-000000000000-0000",
}

macro_rules! invalid_prompt_tests {
    ($($name:ident => $prompt:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                let (_temp, _store, router) = setup();
                let session = router.create_session(None, None).unwrap();
                assert_invalid_input(
                    router.prompt(&session.session_id, $prompt, None).unwrap_err(),
                );
            }
        )+
    };
}

invalid_prompt_tests! {
    prompt_rejects_empty_text => "",
    prompt_rejects_spaces_only => "   ",
    prompt_rejects_tabs_only => "\t\t",
    prompt_rejects_newlines_only => "\n",
    prompt_rejects_oversized_input => "x".repeat(1_000_001).as_str(),
}

macro_rules! invalid_correlation_key_tests {
    ($($name:ident => $key:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                let (_temp, _store, router) = setup();
                assert_invalid_input(
                    router.session_for_correlation_key($key).unwrap_err(),
                );
            }
        )+
    };
}

invalid_correlation_key_tests! {
    correlation_key_too_long_513_bytes => "k".repeat(513).as_str(),
    correlation_key_too_long_1024_bytes => "k".repeat(1024).as_str(),
}

#[tokio::test]
async fn dispatch_rejects_missing_prompt_field() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let error = router
        .dispatch(broker_request(
            "turn.prompt",
            json!({ "sessionId": session.session_id }),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn dispatch_rejects_non_string_model() {
    let (_temp, _store, router) = setup();
    let error = router
        .dispatch(broker_request("turn.create", json!({ "model": 42 })))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn dispatch_rejects_non_string_correlation_key() {
    let (_temp, _store, router) = setup();
    let error = router
        .dispatch(broker_request(
            "turn.create",
            json!({ "correlationKey": [] }),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[tokio::test]
async fn dispatch_rejects_malformed_list_limit() {
    let (_temp, _store, router) = setup();
    let error = router
        .dispatch(broker_request("turn.list", json!({ "limit": "many" })))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[test]
fn invocation_terminal_outcome_serializes_as_durable_status() {
    let outcome = InvocationOutcome::Completed { message_count: 3 };
    let json = serde_json::to_value(outcome).expect("outcome should serialize");
    assert_eq!(json["completed"]["message_count"], 3);
}

#[tokio::test]
async fn prompt_completion_does_not_leave_session_busy() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let first = router.prompt(&session.session_id, "first", None).unwrap();
    let first_id = first.invocation_id.clone();
    collect_events(first).await;
    let second = router.prompt(&session.session_id, "second", None).unwrap();
    assert_ne!(first_id, second.invocation_id);
    collect_events(second).await;
}

#[tokio::test]
async fn event_payloads_carry_tool_identity_and_output() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let events = collect_events(router.prompt(&session.session_id, "hello", None).unwrap()).await;
    let tool_use = events
        .iter()
        .find(|event| event.event_type == SessionEventType::ToolUse)
        .unwrap();
    assert_eq!(tool_use.payload["toolName"], "inspect");
    let tool_result = events
        .iter()
        .find(|event| event.event_type == SessionEventType::ToolResult)
        .unwrap();
    assert_eq!(tool_result.payload["output"], "done");
}

#[tokio::test]
async fn accepted_event_is_transient_while_terminal_event_is_durable() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let events = collect_events(router.prompt(&session.session_id, "hello", None).unwrap()).await;
    assert!(!events.first().unwrap().durable);
    assert!(events.last().unwrap().durable);
}

#[test]
fn store_root_is_scoped_to_the_workspace_session_namespace() {
    let (_temp, store, _router) = setup();
    assert!(store.sessions_dir().to_string_lossy().contains("sessions"));
}

#[tokio::test]
async fn turn_list_accepts_cursor_at_end_of_collection() {
    let (_temp, _store, router) = setup();
    router.create_session(None, None).unwrap();
    let page = router.list(Some("1"), Some(1)).unwrap();
    assert!(page.sessions.is_empty());
    assert_eq!(page.next_cursor, None);
}

#[test]
fn turn_list_accepts_maximum_page_size() {
    let (_temp, _store, router) = setup();
    assert!(router.list(None, Some(100)).is_ok());
}

#[test]
fn turn_list_rejects_overflowing_cursor() {
    let (_temp, _store, router) = setup();
    assert_invalid_input(
        router
            .list(Some("184467440737095516160"), None)
            .unwrap_err(),
    );
}

#[test]
fn terminal_scope_lookup_rejects_oversized_key() {
    let (_temp, _store, router) = setup();
    assert_invalid_input(
        router
            .session_for_correlation_key(&"x".repeat(513))
            .unwrap_err(),
    );
}

#[test]
fn new_router_recovers_only_unfinished_invocations() {
    let (_temp, store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let store_path = store.sessions_dir().join("sdk").join("reconciliation");
    let reconciliation = sdk::InvocationReconciliationStore::new(store_path);
    let terminal_id = Uuid::new_v4().to_string();
    reconciliation
        .insert(InvocationRecord {
            invocation_id: terminal_id.clone(),
            session_id: session.session_id.clone(),
            correlation_key: Some("restored-terminal-scope".to_string()),
            status: InvocationStatus::Terminal,
            accepted_at_ms: 1,
            updated_at_ms: 2,
            outcome: Some(InvocationOutcome::Completed { message_count: 0 }),
            uncertainty_reason: None,
            terminal_event: Some(json!({ "type": "turn_ended" })),
            terminal_scope_revision: Some(1),
        })
        .unwrap();
    let restarted = SessionRouter::new(store, Arc::new(EchoRunner)).unwrap();
    assert_eq!(
        restarted.reconcile(&terminal_id).unwrap().status,
        InvocationStatus::Terminal
    );
    assert_eq!(
        restarted
            .session_for_correlation_key("restored-terminal-scope")
            .unwrap(),
        Some(session.session_id)
    );
}

#[test]
fn terminal_scope_recovery_uses_revision_when_timestamps_tie() {
    let (_temp, store, router) = setup();
    let first = router.create_session(None, Some("revision-tie")).unwrap();
    let latest = router.create_session(None, None).unwrap();
    let reconciliation = sdk::InvocationReconciliationStore::new(
        store.sessions_dir().join("sdk").join("reconciliation"),
    );
    for (invocation_id, session_id, revision) in [
        ("invocation-a", first.session_id.as_str(), 1),
        ("invocation-z", latest.session_id.as_str(), 2),
    ] {
        reconciliation
            .insert(InvocationRecord {
                invocation_id: invocation_id.to_string(),
                session_id: session_id.to_string(),
                correlation_key: Some("revision-tie".to_string()),
                status: InvocationStatus::Terminal,
                accepted_at_ms: 10,
                updated_at_ms: 20,
                outcome: Some(InvocationOutcome::Completed { message_count: 1 }),
                uncertainty_reason: None,
                terminal_event: Some(json!({ "type": "turn_ended" })),
                terminal_scope_revision: Some(revision),
            })
            .unwrap();
    }

    let restarted = SessionRouter::new(store, Arc::new(EchoRunner)).unwrap();
    assert_eq!(
        restarted
            .session_for_correlation_key("revision-tie")
            .unwrap(),
        Some(latest.session_id)
    );
}

#[test]
fn runtime_session_runner_type_is_available_for_runtime_integration() {
    struct EmptyApi;
    impl runtime::ApiClient for EmptyApi {
        fn stream(
            &mut self,
            _request: runtime::ApiRequest,
        ) -> Result<Vec<runtime::AssistantEvent>, runtime::RuntimeError> {
            Ok(vec![runtime::AssistantEvent::TextDelta("done".to_string())])
        }
    }
    let _runner: RuntimeSessionRunner<EmptyApi, runtime::StaticToolExecutor> =
        RuntimeSessionRunner::new(|session| {
            runtime::ConversationRuntime::new(
                session,
                EmptyApi,
                runtime::StaticToolExecutor::default(),
                runtime::PermissionPolicy::new(runtime::PermissionMode::ReadOnly),
                Vec::new(),
            )
        });
}

#[tokio::test]
async fn runtime_runner_streams_real_tool_lifecycle_through_the_router() {
    struct ToolApi {
        calls: usize,
    }

    impl runtime::ApiClient for ToolApi {
        fn stream(
            &mut self,
            _request: runtime::ApiRequest,
        ) -> Result<Vec<runtime::AssistantEvent>, runtime::RuntimeError> {
            self.calls += 1;
            if self.calls == 1 {
                Ok(vec![
                    runtime::AssistantEvent::ToolUse {
                        id: "call-1".to_string(),
                        name: "echo".to_string(),
                        input: "payload".to_string(),
                    },
                    runtime::AssistantEvent::MessageStop,
                ])
            } else {
                Ok(vec![
                    runtime::AssistantEvent::TextDelta("complete".to_string()),
                    runtime::AssistantEvent::MessageStop,
                ])
            }
        }
    }

    let runner = RuntimeSessionRunner::new(|session| {
        runtime::ConversationRuntime::new(
            session,
            ToolApi { calls: 0 },
            runtime::StaticToolExecutor::new().register("echo", |input| Ok(input.to_string())),
            runtime::PermissionPolicy::new(runtime::PermissionMode::DangerFullAccess),
            Vec::new(),
        )
    });
    let (_temp, store, router) = setup_with(Arc::new(runner));
    let session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&session.session_id, "run tool", None)
        .unwrap();
    let invocation_id = invocation.invocation_id.clone();
    let events = collect_events(invocation).await;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            SessionEventType::TurnAccepted,
            SessionEventType::AgentBegin,
            SessionEventType::ToolUse,
            SessionEventType::ToolResult,
            SessionEventType::AgentEnd,
            SessionEventType::TurnEnded,
        ]
    );
    assert_eq!(events[3].payload["output"], "payload");
    assert_eq!(events[4].payload["output"], "complete");
    assert_eq!(
        router.reconcile(&invocation_id).unwrap().outcome,
        Some(InvocationOutcome::Completed { message_count: 4 })
    );
    assert_eq!(
        Session::load_from_path(store.create_handle(&session.session_id).path)
            .unwrap()
            .messages
            .len(),
        4
    );
}

#[tokio::test]
async fn router_steer_reaches_an_active_runtime_before_its_next_model_request() {
    struct BlockingApi {
        calls: usize,
        entered: Arc<Mutex<Option<mpsc::SyncSender<()>>>>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl runtime::ApiClient for BlockingApi {
        fn stream(
            &mut self,
            request: runtime::ApiRequest,
        ) -> Result<Vec<runtime::AssistantEvent>, runtime::RuntimeError> {
            self.calls += 1;
            if self.calls == 1 {
                if let Some(entered) = self
                    .entered
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    let _ = entered.send(());
                }
                self.release
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .recv()
                    .map_err(|error| runtime::RuntimeError::new(error.to_string()))?;
                return Ok(vec![
                    runtime::AssistantEvent::TextDelta("first response".to_string()),
                    runtime::AssistantEvent::MessageStop,
                ]);
            }

            let steer_is_in_history = request.messages.iter().any(|message| {
                message.role == runtime::MessageRole::User
                    && message.blocks.iter().any(|block| {
                        matches!(block, runtime::ContentBlock::Text { text } if text == "midflight steer")
                    })
            });
            if !steer_is_in_history {
                return Err(runtime::RuntimeError::new(
                    "steering prompt was missing from the next API request",
                ));
            }
            Ok(vec![
                runtime::AssistantEvent::TextDelta("steered response".to_string()),
                runtime::AssistantEvent::MessageStop,
            ])
        }
    }

    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let entered = Arc::new(Mutex::new(Some(entered_tx)));
    let release = Arc::new(Mutex::new(release_rx));
    let runner = RuntimeSessionRunner::new({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        move |session| {
            runtime::ConversationRuntime::new(
                session,
                BlockingApi {
                    calls: 0,
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                },
                runtime::StaticToolExecutor::default(),
                runtime::PermissionPolicy::new(runtime::PermissionMode::ReadOnly),
                Vec::new(),
            )
        }
    });
    let (_temp, _store, router) = setup_with(Arc::new(runner));
    let session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&session.session_id, "initial prompt", None)
        .unwrap();
    let invocation_id = invocation.invocation_id.clone();
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first model request should block");
    router
        .steer(&session.session_id, &invocation_id, "midflight steer")
        .expect("active model request should accept steering");
    release_tx.send(()).expect("release first model request");

    let events = collect_events(invocation).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
    assert!(events
        .iter()
        .any(|event| event.event_type == SessionEventType::TurnSteered));
    assert_eq!(
        router.reconcile(&invocation_id).unwrap().status,
        InvocationStatus::Terminal
    );
    assert!(router
        .get_session(&session.session_id)
        .unwrap()
        .messages
        .iter()
        .any(|message| {
            message.role == runtime::MessageRole::User
                && message.blocks.iter().any(|block| {
                    matches!(block, runtime::ContentBlock::Text { text } if text == "midflight steer")
                })
        }));
}

macro_rules! more_invalid_page_cursors {
    ($($name:ident => $cursor:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                let (_temp, _store, router) = setup();
                assert_invalid_input(router.list(Some($cursor), None).unwrap_err());
            }
        )+
    };
}

more_invalid_page_cursors! {
    turn_list_rejects_alphabetic_cursor => "next",
    turn_list_rejects_negative_cursor => "-1",
    turn_list_rejects_fractional_cursor => "1.5",
    turn_list_rejects_blank_cursor => "",
}

#[tokio::test]
async fn invocation_events_use_the_same_id_as_reconciliation_records() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let events = collect_events(router.prompt(&session.session_id, "hello", None).unwrap()).await;
    let invocation_id = events[0].invocation_id.clone();
    assert!(events
        .iter()
        .all(|event| event.invocation_id == invocation_id));
    assert_eq!(
        router.reconcile(&invocation_id).unwrap().invocation_id,
        invocation_id
    );
}

#[test]
fn create_rejects_oversized_correlation_key_before_writing_session() {
    let (_temp, store, router) = setup();
    let error = router
        .create_session(None, Some(&"c".repeat(513)))
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(store.list_sessions().unwrap().is_empty());
}

#[tokio::test]
async fn turn_prompt_terminal_outcome_is_available_after_event_receiver_drops() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let invocation = router.prompt(&session.session_id, "hello", None).unwrap();
    let invocation_id = invocation.invocation_id.clone();
    drop(invocation);
    let terminal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(record) = router.reconcile(&invocation_id) {
                if record.status == InvocationStatus::Terminal {
                    return record;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("invocation should eventually become terminal");
    assert_eq!(terminal.status, InvocationStatus::Terminal);
}

#[tokio::test]
async fn turn_prompt_event_stream_closes_after_terminal_event() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let events = collect_events(router.prompt(&session.session_id, "hello", None).unwrap()).await;
    assert_eq!(
        events.last().unwrap().event_type,
        SessionEventType::TurnEnded
    );
}

#[tokio::test]
async fn abort_marks_terminal_event_durable() {
    let runner = Arc::new(AbortRunner {
        calls: AtomicUsize::new(0),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router.create_session(None, None).unwrap();
    let previous = router.prompt(&session.session_id, "long", None).unwrap();
    let previous_id = previous.invocation_id.clone();
    let replacement = router
        .abort_and_prompt(&session.session_id, "next", None)
        .await
        .unwrap();
    let old_events = collect_events(previous).await;
    let terminal = old_events.last().unwrap();
    assert_eq!(terminal.event_type, SessionEventType::TurnEnded);
    assert!(terminal.durable);
    assert_eq!(
        router.reconcile(&previous_id).unwrap().outcome,
        Some(InvocationOutcome::Aborted { message_count: 0 })
    );
    drop(replacement);
}

#[tokio::test]
async fn prompt_session_messages_are_durable_before_terminal_event() {
    let (_temp, _store, router) = setup();
    let session = router.create_session(None, None).unwrap();
    let invocation = router
        .prompt(&session.session_id, "persist me", None)
        .unwrap();
    collect_events(invocation).await;
    let loaded = router.get_session(&session.session_id).unwrap();
    assert_eq!(loaded.messages.len(), 1);
}

#[test]
fn session_id_lookup_rejects_uuid_that_is_not_a_persisted_session() {
    let (_temp, _store, router) = setup();
    assert_eq!(
        router
            .get_session(&Uuid::new_v4().to_string())
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn turn_create_empty_prompt_is_rejected_without_orphaning_session() {
    let (_temp, store, router) = setup();
    let error = router
        .dispatch(broker_request("turn.create", json!({ "input": " " })))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(store.list_sessions().unwrap().is_empty());
}

#[tokio::test]
async fn turn_steer_event_is_sequenced_after_acceptance() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let runner = Arc::new(GateRunner {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(release_rx),
    });
    let (_temp, _store, router) = setup_with(runner);
    let session = router.create_session(None, None).unwrap();
    let invocation = router.prompt(&session.session_id, "initial", None).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    router
        .steer(&session.session_id, &invocation.invocation_id, "override")
        .unwrap();
    release_tx.send(()).unwrap();
    let events = collect_events(invocation).await;
    assert!(events[0].sequence < events[1].sequence);
    let steer_index = events
        .iter()
        .position(|event| event.event_type == SessionEventType::TurnSteered)
        .unwrap();
    assert!(steer_index > 0);
}

#[tokio::test]
async fn concurrent_steer_and_completion_preserve_sequence_and_terminal_order() {
    for _ in 0..16 {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let runner = Arc::new(GateRunner {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
        });
        let (_temp, _store, router) = setup_with(runner);
        let session = router.create_session(None, None).unwrap();
        let invocation = router.prompt(&session.session_id, "initial", None).unwrap();
        let invocation_id = invocation.invocation_id.clone();
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let barrier = Arc::new(Barrier::new(3));
        let steer_router = router.clone();
        let steer_session_id = session.session_id.clone();
        let steer_invocation_id = invocation_id.clone();
        let steer_barrier = Arc::clone(&barrier);
        let steer_thread = thread::spawn(move || {
            steer_barrier.wait();
            steer_router
                .steer(&steer_session_id, &steer_invocation_id, "racing steer")
                .is_ok()
        });
        let finish_barrier = Arc::clone(&barrier);
        let finish_thread = thread::spawn(move || {
            finish_barrier.wait();
            release_tx.send(()).unwrap();
        });
        barrier.wait();

        let steer_accepted = steer_thread.join().unwrap();
        finish_thread.join().unwrap();
        let events = collect_events(invocation).await;
        assert!(events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
        assert_eq!(
            events.last().unwrap().event_type,
            SessionEventType::TurnEnded
        );
        let steer_index = events
            .iter()
            .position(|event| event.event_type == SessionEventType::TurnSteered);
        assert_eq!(steer_accepted, steer_index.is_some());
        if let Some(index) = steer_index {
            assert!(index < events.len() - 1);
        }
    }
}

#[test]
fn session_page_types_round_trip_as_json() {
    let page = sdk::SessionPage {
        sessions: vec![sdk::SessionSummary {
            session_id: "session-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            message_count: 3,
        }],
        next_cursor: Some("1".to_string()),
    };
    let encoded = serde_json::to_value(&page).unwrap();
    let decoded: sdk::SessionPage = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, page);
}

#[test]
fn session_event_type_uses_protocol_snake_case() {
    assert_eq!(
        serde_json::to_string(&SessionEventType::TurnAccepted).unwrap(),
        "\"turn_accepted\""
    );
}
