//! Reconnecting WebSocket client for the SDK wire protocol.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;
use uuid::Uuid;

use crate::error::{SdkError, SdkErrorCode, SdkResult, SdkSentRecord};
use crate::frame::{SdkFrame, MAX_FRAME_BYTES, PROTOCOL_VERSION};

const MAX_SENT_RECORDS: usize = 256;
const CLOSE_GRACE: Duration = Duration::from_secs(5);
const CANCEL_ACK_GRACE: Duration = Duration::from_millis(100);

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runtime options for an SDK WebSocket client.
#[derive(Debug, Clone)]
pub struct SdkClientConfig {
    /// WebSocket endpoint (`ws://` or `wss://`).
    pub url: String,
    /// Broker authentication token, sent as the `token` query parameter.
    pub token: String,
    /// Maximum UTF-8 frame size accepted in either direction.
    pub max_frame_bytes: usize,
    /// Default timeout for an individual request.
    pub request_timeout: Duration,
    /// Timeout for each socket-open and hello-handshake attempt.
    pub connect_timeout: Duration,
    /// Maximum time spent across a connection or reconnect cycle.
    pub reconnect_deadline: Duration,
    /// Number of retries after the initial connection attempt.
    pub reconnect_attempts: u32,
    /// Initial reconnect delay; subsequent delays double up to `reconnect_max_backoff`.
    pub reconnect_initial_backoff: Duration,
    /// Maximum reconnect delay.
    pub reconnect_max_backoff: Duration,
    /// Capabilities sent in the protocol-v3 client hello.
    pub capabilities: Vec<String>,
}

impl Default for SdkClientConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:8080".to_string(),
            token: String::new(),
            max_frame_bytes: MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            reconnect_deadline: Duration::from_secs(30),
            reconnect_attempts: 3,
            reconnect_initial_backoff: Duration::from_millis(25),
            reconnect_max_backoff: Duration::from_secs(2),
            capabilities: Vec::new(),
        }
    }
}

impl SdkClientConfig {
    fn validate(&self) -> SdkResult<()> {
        let endpoint = Url::parse(&self.url)
            .map_err(|error| SdkError::invalid_input(format!("invalid WebSocket URL: {error}")))?;
        if !matches!(endpoint.scheme(), "ws" | "wss") {
            return Err(SdkError::invalid_input(
                "SDK endpoint must use the ws:// or wss:// scheme",
            ));
        }
        if self.max_frame_bytes == 0 {
            return Err(SdkError::invalid_input(
                "maximum frame size must be greater than zero",
            ));
        }
        if self.connect_timeout.is_zero() || self.reconnect_deadline.is_zero() {
            return Err(SdkError::invalid_input(
                "connection timeout and reconnect deadline must be greater than zero",
            ));
        }
        if self.reconnect_max_backoff < self.reconnect_initial_backoff {
            return Err(SdkError::invalid_input(
                "maximum reconnect backoff must be at least the initial backoff",
            ));
        }
        Ok(())
    }
}

struct SentRecordStore {
    records: HashMap<Uuid, SdkSentRecord>,
    order: VecDeque<Uuid>,
}

impl SentRecordStore {
    fn new() -> Self {
        Self {
            records: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, record: SdkSentRecord) {
        if self.records.contains_key(&record.id) {
            self.order.retain(|id| *id != record.id);
        }
        self.order.push_back(record.id);
        self.records.insert(record.id, record);
        while self.records.len() > MAX_SENT_RECORDS {
            if let Some(oldest) = self.order.pop_front() {
                self.records.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, id: Uuid) {
        self.records.remove(&id);
        self.order.retain(|record_id| *record_id != id);
    }

    fn get(&self, id: Uuid) -> Option<SdkSentRecord> {
        self.records.get(&id).cloned()
    }

    fn all(&self) -> Vec<SdkSentRecord> {
        self.order
            .iter()
            .filter_map(|id| self.records.get(id).cloned())
            .collect()
    }
}

struct RequestMessage {
    id: Uuid,
    operation: Option<String>,
    serialized: String,
    deadline: Instant,
    response_tx: oneshot::Sender<SdkResult<SdkFrame>>,
}

enum ClientCommand {
    Request(RequestMessage),
    Cancel {
        id: Uuid,
        ack: oneshot::Sender<Option<SdkSentRecord>>,
    },
}

struct PendingRequest {
    response_tx: oneshot::Sender<SdkResult<SdkFrame>>,
    sent_record: SdkSentRecord,
}

struct ConnectedSocket {
    socket: ClientSocket,
    connection_id: Option<String>,
}

/// SDK client with UUID correlation, handshake, timeout handling, and reconnects.
pub struct SdkClient {
    config: SdkClientConfig,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    shutdown_tx: watch::Sender<bool>,
    task_handle: AsyncMutex<Option<JoinHandle<()>>>,
    sent_records: Arc<Mutex<SentRecordStore>>,
    connection_id: Arc<RwLock<Option<String>>>,
    closed: AtomicBool,
}

impl fmt::Debug for SdkClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SdkClient")
            .field("config", &self.config)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl SdkClient {
    /// Connect, complete the protocol-v3 hello handshake, and start reconnect supervision.
    pub async fn connect(config: SdkClientConfig) -> SdkResult<Self> {
        config.validate()?;
        let connected = connect_with_retry(&config).await?;
        let connection_id = Arc::new(RwLock::new(connected.connection_id.clone()));
        let sent_records = Arc::new(Mutex::new(SentRecordStore::new()));
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task_handle = tokio::spawn(run_client(
            connected,
            config.clone(),
            command_rx,
            shutdown_rx,
            sent_records.clone(),
            connection_id.clone(),
        ));

        Ok(Self {
            config,
            command_tx,
            shutdown_tx,
            task_handle: AsyncMutex::new(Some(task_handle)),
            sent_records,
            connection_id,
            closed: AtomicBool::new(false),
        })
    }

    /// Send a broker operation with the configured default request timeout.
    pub async fn request(&self, operation: impl Into<String>, input: Value) -> SdkResult<SdkFrame> {
        self.request_with_timeout(operation, input, self.config.request_timeout)
            .await
    }

    /// Send a broker operation with a per-request timeout override.
    pub async fn request_with_timeout(
        &self,
        operation: impl Into<String>,
        input: Value,
        request_timeout: Duration,
    ) -> SdkResult<SdkFrame> {
        let operation = operation.into();
        let frame = SdkFrame::broker_request(Uuid::new_v4(), operation, input);
        self.request_frame_with_timeout(frame, request_timeout)
            .await
    }

    /// Send a typed frame, replacing its `id` with a fresh UUID for correlation.
    pub async fn request_frame(&self, frame: SdkFrame) -> SdkResult<SdkFrame> {
        self.request_frame_with_timeout(frame, self.config.request_timeout)
            .await
    }

    /// Send a typed frame with a per-request timeout override.
    pub async fn request_frame_with_timeout(
        &self,
        mut frame: SdkFrame,
        request_timeout: Duration,
    ) -> SdkResult<SdkFrame> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SdkError::new(
                SdkErrorCode::ConnectionClosed,
                "SDK client is closed",
            ));
        }
        let id = Uuid::new_v4();
        frame.set_id(id);
        let operation = frame.operation();
        let serialized = frame.to_json()?;
        if serialized.len() > self.config.max_frame_bytes {
            return Err(SdkError::payload_too_large(format!(
                "frame size {} exceeds maximum {}",
                serialized.len(),
                self.config.max_frame_bytes
            )));
        }

        let deadline = Instant::now() + request_timeout;
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Request(RequestMessage {
                id,
                operation,
                serialized,
                deadline,
                response_tx,
            }))
            .map_err(|_| {
                SdkError::new(
                    SdkErrorCode::ConnectionClosed,
                    "SDK connection task is no longer running",
                )
            })?;

        match timeout_at(deadline, response_rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => Err(SdkError::connection_error(
                "SDK connection task dropped the response channel",
            )),
            Err(_) => {
                let (ack_tx, ack_rx) = oneshot::channel();
                let _ = self
                    .command_tx
                    .send(ClientCommand::Cancel { id, ack: ack_tx });
                let acknowledged = timeout(CANCEL_ACK_GRACE, ack_rx)
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten();
                let sent_record = acknowledged.or_else(|| self.get_sent_record(id));
                match sent_record {
                    Some(record) => Err(SdkError::uncertain_after_send(record)),
                    None => Err(SdkError::timeout(format!(
                        "SDK request timed out after {}ms before a response was received",
                        request_timeout.as_millis()
                    ))),
                }
            }
        }
    }

    /// Current broker connection identifier from the most recent hello frame.
    pub async fn connection_id(&self) -> Option<String> {
        self.connection_id.read().await.clone()
    }

    /// Look up a retained request identity after an uncertain send.
    pub fn get_sent_record(&self, id: Uuid) -> Option<SdkSentRecord> {
        self.sent_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
    }

    /// List outstanding sent records that require caller-side reconciliation.
    pub fn uncertain_sends(&self) -> Vec<SdkSentRecord> {
        self.sent_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .all()
    }

    /// Forget a request record after the caller has reconciled its outcome.
    pub fn forget_sent_record(&self, id: Uuid) -> bool {
        let mut records = self
            .sent_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = records.records.contains_key(&id);
        records.remove(id);
        found
    }

    /// Close the socket and stop reconnect attempts. Safe to call more than once.
    pub async fn close(&self) -> SdkResult<()> {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.shutdown_tx.send(true);
        }
        if let Some(mut task) = self.task_handle.lock().await.take() {
            match timeout(CLOSE_GRACE, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(SdkError::connection_error(format!(
                        "SDK connection task failed while closing: {error}"
                    )))
                }
                Err(_) => {
                    task.abort();
                    return Err(SdkError::timeout(
                        "SDK connection task did not stop in time",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Drop for SdkClient {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.shutdown_tx.send(true);
        }
    }
}

fn authenticated_url(config: &SdkClientConfig) -> SdkResult<Url> {
    let mut url = Url::parse(&config.url)
        .map_err(|error| SdkError::invalid_input(format!("invalid WebSocket URL: {error}")))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(SdkError::invalid_input(
            "SDK endpoint must use the ws:// or wss:// scheme",
        ));
    }
    let mut query: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| key != "token")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    query.push(("token".to_string(), config.token.clone()));
    url.set_query(None);
    url.query_pairs_mut().extend_pairs(query);
    Ok(url)
}

async fn connect_with_retry(config: &SdkClientConfig) -> SdkResult<ConnectedSocket> {
    let deadline = Instant::now() + config.reconnect_deadline;
    let mut last_error = None;

    for attempt in 0..=config.reconnect_attempts {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(SdkError::timeout("SDK reconnect deadline elapsed"));
        }
        let attempt_timeout = remaining.min(config.connect_timeout);
        match timeout(attempt_timeout, connect_once(config)).await {
            Ok(Ok(connected)) => return Ok(connected),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(SdkError::timeout(format!(
                    "SDK connection attempt timed out after {}ms",
                    attempt_timeout.as_millis()
                )))
            }
        }

        if attempt == config.reconnect_attempts {
            break;
        }
        let delay = reconnect_delay(config, attempt);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(SdkError::timeout("SDK reconnect deadline elapsed"));
        }
        sleep(delay.min(remaining)).await;
    }

    let cause = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "no connection attempt was made".to_string());
    Err(SdkError::new(
        SdkErrorCode::ReconnectExhausted,
        format!("SDK WebSocket reconnect attempts exhausted: {cause}"),
    ))
}

fn reconnect_delay(config: &SdkClientConfig, attempt: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(attempt).unwrap_or(u32::MAX);
    config
        .reconnect_initial_backoff
        .saturating_mul(multiplier)
        .min(config.reconnect_max_backoff)
}

async fn connect_once(config: &SdkClientConfig) -> SdkResult<ConnectedSocket> {
    let url = authenticated_url(config)?;
    let (mut socket, _) = connect_async(url.as_str()).await.map_err(|error| {
        SdkError::connection_error(format!("WebSocket connection failed: {error}"))
    })?;

    let hello = SdkFrame::client_hello(&config.capabilities).to_json()?;
    socket.send(Message::Text(hello)).await.map_err(|error| {
        SdkError::connection_error(format!("client hello send failed: {error}"))
    })?;

    let handshake_deadline = Instant::now() + config.connect_timeout;
    loop {
        let message = timeout_at(handshake_deadline, socket.next())
            .await
            .map_err(|_| SdkError::timeout("SDK server hello handshake timed out"))?
            .ok_or_else(|| SdkError::connection_error("WebSocket closed before server hello"))?
            .map_err(|error| {
                SdkError::connection_error(format!("WebSocket failed before server hello: {error}"))
            })?;
        match message {
            Message::Text(text) => {
                if text.len() > config.max_frame_bytes {
                    return Err(SdkError::payload_too_large(format!(
                        "server hello frame size {} exceeds maximum {}",
                        text.len(),
                        config.max_frame_bytes
                    )));
                }
                let frame = SdkFrame::from_json(&text)?;
                if frame.is_server_hello() {
                    if let Some(version) = frame.data.get("protocolVersion").and_then(Value::as_u64)
                    {
                        if version != u64::from(PROTOCOL_VERSION) {
                            return Err(SdkError::new(
                                SdkErrorCode::ProtocolError,
                                format!(
                                    "server protocol version {version} does not match {PROTOCOL_VERSION}"
                                ),
                            ));
                        }
                    }
                    let connection_id = frame
                        .connection_id()
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned);
                    return Ok(ConnectedSocket {
                        socket,
                        connection_id,
                    });
                }
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.map_err(|error| {
                    SdkError::connection_error(format!("WebSocket pong failed: {error}"))
                })?
            }
            Message::Close(_) => {
                return Err(SdkError::connection_error(
                    "WebSocket closed before server hello",
                ));
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

async fn await_reconnect(
    reconnect_task: &mut Option<JoinHandle<SdkResult<ConnectedSocket>>>,
) -> SdkResult<ConnectedSocket> {
    match reconnect_task {
        Some(task) => task.await.map_err(|error| {
            SdkError::connection_error(format!("SDK reconnect task failed: {error}"))
        })?,
        None => pending().await,
    }
}

async fn run_client(
    connected: ConnectedSocket,
    config: SdkClientConfig,
    mut command_rx: mpsc::UnboundedReceiver<ClientCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
    sent_records: Arc<Mutex<SentRecordStore>>,
    connection_id: Arc<RwLock<Option<String>>>,
) {
    let mut socket = Some(connected.socket);
    let mut queued = VecDeque::new();
    let mut pending_requests = HashMap::new();
    let mut reconnect_task = None;

    loop {
        let incoming = async {
            match socket.as_mut() {
                Some(connected) => connected.next().await,
                None => pending().await,
            }
        };
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            message = incoming => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > config.max_frame_bytes {
                            tracing::warn!(size = text.len(), "SDK server frame exceeds configured limit");
                            fail_pending_uncertain(&mut pending_requests);
                            start_reconnect(&config, &mut socket, &mut reconnect_task);
                        } else {
                            handle_server_text(&text, &mut pending_requests, &sent_records, &connection_id).await;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let send_result = match socket.as_mut() {
                            Some(connected) => connected.send(Message::Pong(payload)).await,
                            None => continue,
                        };
                        if send_result.is_err() {
                            fail_pending_uncertain(&mut pending_requests);
                            start_reconnect(&config, &mut socket, &mut reconnect_task);
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        fail_pending_uncertain(&mut pending_requests);
                        start_reconnect(&config, &mut socket, &mut reconnect_task);
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else { break };
                match command {
                    ClientCommand::Request(request) => {
                        if socket.is_none() {
                            queued.push_back(request);
                        } else if !send_request(
                            request,
                            socket.as_mut().expect("socket checked above"),
                            &mut pending_requests,
                            &sent_records,
                        ).await {
                            fail_pending_uncertain(&mut pending_requests);
                            start_reconnect(&config, &mut socket, &mut reconnect_task);
                        }
                    }
                    ClientCommand::Cancel { id, ack } => {
                        let record = cancel_request(
                            id,
                            &mut queued,
                            &mut pending_requests,
                            &sent_records,
                        );
                        let _ = ack.send(record);
                    }
                }
            }
            result = await_reconnect(&mut reconnect_task) => {
                reconnect_task = None;
                match result {
                    Ok(connected) => {
                        *connection_id.write().await = connected.connection_id;
                        socket = Some(connected.socket);
                        while let Some(request) = queued.pop_front() {
                            if !send_request(
                                request,
                                socket.as_mut().expect("reconnected socket is present"),
                                &mut pending_requests,
                                &sent_records,
                            ).await {
                                fail_pending_uncertain(&mut pending_requests);
                                start_reconnect(&config, &mut socket, &mut reconnect_task);
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        for request in queued.drain(..) {
                            let _ = request.response_tx.send(Err(error.clone()));
                        }
                        break;
                    }
                }
            }
        }
    }

    if let Some(task) = reconnect_task {
        task.abort();
    }
    fail_pending_uncertain(&mut pending_requests);
    for request in queued.drain(..) {
        let _ = request.response_tx.send(Err(SdkError::new(
            SdkErrorCode::ConnectionClosed,
            "SDK client closed before request was sent",
        )));
    }
    if let Some(mut connected) = socket {
        let _ = timeout(CLOSE_GRACE, connected.close(None)).await;
    }
}

fn start_reconnect(
    config: &SdkClientConfig,
    socket: &mut Option<ClientSocket>,
    reconnect_task: &mut Option<JoinHandle<SdkResult<ConnectedSocket>>>,
) {
    socket.take();
    if reconnect_task.is_none() {
        let config = config.clone();
        *reconnect_task = Some(tokio::spawn(
            async move { connect_with_retry(&config).await },
        ));
    }
}

async fn send_request(
    request: RequestMessage,
    socket: &mut ClientSocket,
    pending_requests: &mut HashMap<Uuid, PendingRequest>,
    sent_records: &Arc<Mutex<SentRecordStore>>,
) -> bool {
    if Instant::now() >= request.deadline {
        let _ = request.response_tx.send(Err(SdkError::timeout(
            "SDK request deadline elapsed before send",
        )));
        return true;
    }

    let sent_record = SdkSentRecord {
        id: request.id,
        operation: request.operation,
        sent_at: SystemTime::now(),
    };
    sent_records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(sent_record.clone());
    pending_requests.insert(
        request.id,
        PendingRequest {
            response_tx: request.response_tx,
            sent_record: sent_record.clone(),
        },
    );

    let send_result = timeout_at(
        request.deadline,
        socket.send(Message::Text(request.serialized)),
    )
    .await;
    match send_result {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            if let Some(pending) = pending_requests.remove(&request.id) {
                let _ = pending
                    .response_tx
                    .send(Err(SdkError::uncertain_after_send(pending.sent_record)));
            }
            tracing::warn!(%error, "SDK request send failed after dispatch began");
            false
        }
        Err(_) => {
            if let Some(pending) = pending_requests.remove(&request.id) {
                let _ = pending
                    .response_tx
                    .send(Err(SdkError::uncertain_after_send(pending.sent_record)));
            }
            false
        }
    }
}

fn cancel_request(
    id: Uuid,
    queued: &mut VecDeque<RequestMessage>,
    pending_requests: &mut HashMap<Uuid, PendingRequest>,
    sent_records: &Arc<Mutex<SentRecordStore>>,
) -> Option<SdkSentRecord> {
    if let Some(position) = queued.iter().position(|request| request.id == id) {
        queued.remove(position);
        return None;
    }
    if let Some(pending) = pending_requests.remove(&id) {
        return Some(pending.sent_record);
    }
    sent_records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(id)
}

async fn handle_server_text(
    text: &str,
    pending_requests: &mut HashMap<Uuid, PendingRequest>,
    sent_records: &Arc<Mutex<SentRecordStore>>,
    connection_id: &Arc<RwLock<Option<String>>>,
) {
    let frame = match SdkFrame::from_json(text) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(%error, "SDK server sent malformed JSON frame");
            return;
        }
    };
    if frame.is_server_hello() {
        if let Some(id) = frame.connection_id().filter(|id| !id.is_empty()) {
            *connection_id.write().await = Some(id.to_string());
        }
        return;
    }

    let id = match frame.response_id() {
        Ok(Some(id)) => id,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%error, "SDK server sent an invalid correlation ID");
            return;
        }
    };
    if let Some(pending) = pending_requests.remove(&id) {
        sent_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        let result = if frame.data.get("ok") == Some(&Value::Bool(false))
            || frame.data.get("status").and_then(Value::as_str) == Some("error")
        {
            Err(error_from_response(&frame))
        } else {
            Ok(frame)
        };
        let _ = pending.response_tx.send(result);
    }
}

fn error_from_response(frame: &SdkFrame) -> SdkError {
    match frame.error() {
        Some(Ok((code, message))) => SdkError::from_remote(&code, message),
        Some(Err(error)) => error,
        None => SdkError::new(
            SdkErrorCode::Unavailable,
            "SDK request failed without error details",
        ),
    }
}

fn fail_pending_uncertain(pending_requests: &mut HashMap<Uuid, PendingRequest>) {
    for (_, pending) in pending_requests.drain() {
        let _ = pending
            .response_tx
            .send(Err(SdkError::uncertain_after_send(pending.sent_record)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::WebSocketStream as TestWebSocketStream;

    type ServerSocket = TestWebSocketStream<TcpStream>;

    async fn test_server() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        (listener, format!("ws://{address}/sdk?keep=1"))
    }

    fn test_config(url: String) -> SdkClientConfig {
        SdkClientConfig {
            url,
            token: "token with & symbols".to_string(),
            connect_timeout: Duration::from_millis(250),
            reconnect_deadline: Duration::from_secs(2),
            reconnect_attempts: 1,
            reconnect_initial_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            ..SdkClientConfig::default()
        }
    }

    async fn accept(listener: &TcpListener) -> ServerSocket {
        let (stream, _) = listener.accept().await.unwrap();
        accept_async(stream).await.unwrap()
    }

    async fn read_frame(socket: &mut ServerSocket) -> SdkFrame {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => return SdkFrame::from_json(&text).unwrap(),
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
                _ => {}
            }
        }
    }

    async fn assert_no_text_frame(socket: &mut ServerSocket) {
        if let Ok(Some(Ok(Message::Text(_)))) =
            timeout(Duration::from_millis(50), socket.next()).await
        {
            panic!("client unexpectedly dispatched a request frame");
        }
    }

    async fn complete_hello(socket: &mut ServerSocket, connection_id: &str) -> SdkFrame {
        let hello = read_frame(socket).await;
        assert_eq!(hello.frame_type().unwrap(), crate::frame::FrameType::Hello);
        socket
            .send(Message::Text(
                json!({"type":"hello", "connectionId":connection_id}).to_string(),
            ))
            .await
            .unwrap();
        hello
    }

    async fn send_response(socket: &mut ServerSocket, id: Uuid, result: Value) {
        socket
            .send(Message::Text(
                json!({"type":"broker_response", "id":id, "ok":true, "result":result}).to_string(),
            ))
            .await
            .unwrap();
    }

    #[test]
    fn config_defaults_use_ten_second_timeout_and_bounded_backoff() {
        let config = SdkClientConfig::default();
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.max_frame_bytes, MAX_FRAME_BYTES);
        let delays: Vec<_> = (0..8)
            .map(|attempt| reconnect_delay(&config, attempt))
            .collect();
        assert_eq!(
            delays,
            [
                Duration::from_millis(25),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(800),
                Duration::from_millis(1_600),
                Duration::from_secs(2),
            ]
        );
    }

    #[test]
    fn auth_url_preserves_query_and_encodes_token_once() {
        let config = SdkClientConfig {
            url: "wss://sdk.example/path?keep=a%20b&token=stale".to_string(),
            token: "a token&value".to_string(),
            ..SdkClientConfig::default()
        };
        let url = authenticated_url(&config).unwrap();
        let pairs: Vec<_> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert_eq!(
            pairs,
            [
                ("keep".to_string(), "a b".to_string()),
                ("token".to_string(), "a token&value".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn rejects_non_websocket_endpoint_schemes() {
        let config = SdkClientConfig {
            url: "https://sdk.example".to_string(),
            ..SdkClientConfig::default()
        };
        assert_eq!(
            SdkClient::connect(config).await.unwrap_err().code,
            SdkErrorCode::InvalidInput
        );
    }

    #[tokio::test]
    async fn handshake_advertises_capabilities_and_correlates_response_uuid() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let hello = complete_hello(&mut socket, "conn-1").await;
            assert_eq!(hello.data["protocolVersion"], PROTOCOL_VERSION);
            assert_eq!(hello.data["capabilities"], json!(["session.observe.v1"]));
            let request = read_frame(&mut socket).await;
            let id = request.id().unwrap().unwrap();
            assert_eq!(request.operation().as_deref(), Some("session.get"));
            send_response(&mut socket, id, json!({"session": 7})).await;
        });
        let mut config = test_config(url);
        config.capabilities = vec!["session.observe.v1".to_string()];
        let client = SdkClient::connect(config).await.unwrap();
        assert_eq!(client.connection_id().await.as_deref(), Some("conn-1"));
        let response = client
            .request("session.get", json!({"id": "s-1"}))
            .await
            .unwrap();
        assert_eq!(response.result(), Some(json!({"session": 7})));
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn requests_are_correlated_when_responses_arrive_out_of_order() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "concurrent").await;
            let first = read_frame(&mut socket).await;
            let second = read_frame(&mut socket).await;
            let first_id = first.id().unwrap().unwrap();
            let second_id = second.id().unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"broker_response", "id":second_id, "ok":true, "result":"second"})
                        .to_string(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"broker_response", "id":first_id, "ok":true, "result":"first"})
                        .to_string(),
                ))
                .await
                .unwrap();
        });
        let client = SdkClient::connect(test_config(url)).await.unwrap();
        let first = client.request("op.first", json!({}));
        let second = client.request("op.second", json!({}));
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().result(), Some(json!("first")));
        assert_eq!(second.unwrap().result(), Some(json!("second")));
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn broker_errors_are_returned_with_typed_codes() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "errors").await;
            let request = read_frame(&mut socket).await;
            let id = request.id().unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"broker_response", "id":id, "ok":false, "error":{"code":"not_found", "message":"missing"}})
                        .to_string(),
                ))
                .await
                .unwrap();
        });
        let client = SdkClient::connect(test_config(url)).await.unwrap();
        let error = client
            .request("session.missing", json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, SdkErrorCode::NotFound);
        assert_eq!(error.message, "missing");
        assert!(error.sent_record.is_none());
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_override_marks_sent_request_uncertain_and_retains_record() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "timeout").await;
            let request = read_frame(&mut socket).await;
            assert!(request.id().unwrap().is_some());
            tokio::time::sleep(Duration::from_millis(80)).await;
        });
        let mut config = test_config(url);
        config.request_timeout = Duration::from_secs(1);
        let client = SdkClient::connect(config).await.unwrap();
        let error = client
            .request_with_timeout(
                "session.create",
                json!({"secret": "not retained"}),
                Duration::from_millis(20),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, SdkErrorCode::UncertainAfterSend);
        let record = error.sent_record.unwrap();
        assert_eq!(record.operation.as_deref(), Some("session.create"));
        assert_eq!(client.get_sent_record(record.id), Some(record.clone()));
        assert_eq!(client.uncertain_sends(), vec![record.clone()]);
        assert!(client.forget_sent_record(record.id));
        assert!(client.get_sent_record(record.id).is_none());
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_outgoing_frame_is_rejected_before_dispatch() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "size").await;
            assert_no_text_frame(&mut socket).await;
        });
        let mut config = test_config(url);
        config.max_frame_bytes = 128;
        let client = SdkClient::connect(config).await.unwrap();
        let error = client
            .request("large", json!({"payload": "x".repeat(256)}))
            .await
            .unwrap_err();
        assert_eq!(error.code, SdkErrorCode::PayloadTooLarge);
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnects_after_close_and_never_replays_uncertain_request() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut first = accept(&listener).await;
            complete_hello(&mut first, "first").await;
            let uncertain = read_frame(&mut first).await;
            let uncertain_id = uncertain.id().unwrap().unwrap();
            first.send(Message::Close(None)).await.unwrap();
            drop(first);

            let mut second = accept(&listener).await;
            complete_hello(&mut second, "second").await;
            let next = read_frame(&mut second).await;
            let next_id = next.id().unwrap().unwrap();
            assert_ne!(next_id, uncertain_id);
            assert_eq!(next.operation().as_deref(), Some("after.reconnect"));
            send_response(&mut second, next_id, json!({"connected": true})).await;
        });
        let client = SdkClient::connect(test_config(url)).await.unwrap();
        let uncertain = client.request("uncertain.mutation", json!({}));
        let error = uncertain.await.unwrap_err();
        assert_eq!(error.code, SdkErrorCode::UncertainAfterSend);
        let next = client.request("after.reconnect", json!({})).await.unwrap();
        assert_eq!(next.result(), Some(json!({"connected": true})));
        assert_eq!(client.connection_id().await.as_deref(), Some("second"));
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_failed_initial_connections_with_exponential_backoff() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let (failed_stream, _) = listener.accept().await.unwrap();
            drop(failed_stream);
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let hello = complete_hello(&mut socket, "retry-success").await;
            assert_eq!(hello.frame_type().unwrap(), crate::frame::FrameType::Hello);
        });
        let mut config = test_config(url);
        config.reconnect_attempts = 2;
        let client = SdkClient::connect(config).await.unwrap();
        assert_eq!(
            client.connection_id().await.as_deref(),
            Some("retry-success")
        );
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connection_attempts_stop_at_the_reconnect_deadline() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let _socket = accept(&listener).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let mut config = test_config(url);
        config.connect_timeout = Duration::from_millis(500);
        config.reconnect_deadline = Duration::from_millis(25);
        config.reconnect_attempts = 8;
        let error = SdkClient::connect(config).await.unwrap_err();
        assert_eq!(error.code, SdkErrorCode::Timeout);
        assert!(error.message.contains("deadline"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exhausted_reconnect_budget_returns_typed_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut config = test_config(url);
        config.reconnect_attempts = 0;
        let error = SdkClient::connect(config).await.unwrap_err();
        assert_eq!(error.code, SdkErrorCode::ReconnectExhausted);
    }

    #[tokio::test]
    async fn request_timeout_before_send_does_not_create_uncertain_record() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "immediate-timeout").await;
            assert_no_text_frame(&mut socket).await;
        });
        let client = SdkClient::connect(test_config(url)).await.unwrap();
        let error = client
            .request_with_timeout("too.short", json!({}), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(error.code, SdkErrorCode::Timeout);
        assert!(client.uncertain_sends().is_empty());
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_server_frames_do_not_settle_unrelated_requests() {
        let (listener, url) = test_server().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            complete_hello(&mut socket, "malformed").await;
            let request = read_frame(&mut socket).await;
            let id = request.id().unwrap().unwrap();
            socket
                .send(Message::Text("not-json".to_string()))
                .await
                .unwrap();
            send_response(&mut socket, id, json!("ok-after-malformed")).await;
        });
        let client = SdkClient::connect(test_config(url)).await.unwrap();
        let response = client.request("malformed.noise", json!({})).await.unwrap();
        assert_eq!(response.result(), Some(json!("ok-after-malformed")));
        client.close().await.unwrap();
        server.await.unwrap();
    }

    async fn round_trip_case(case: u8) {
        let (listener, url) = test_server().await;
        let operation = format!("round.trip.case_{case:03}");
        let input = json!({"case": case, "value": format!("request-{case}")});
        let server_operation = operation.clone();
        let server_input = input.clone();
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let hello = complete_hello(&mut socket, "matrix").await;
            assert_eq!(
                hello.data["capabilities"],
                json!([format!("matrix.{case}")])
            );

            let request = read_frame(&mut socket).await;
            let id = request.id().unwrap().expect("request has a UUID ID");
            assert_eq!(request.operation(), Some(server_operation));
            assert_eq!(request.input(), Some(server_input));
            send_response(&mut socket, id, json!({"echo": format!("request-{case}")})).await;
        });

        let mut config = test_config(url);
        config.capabilities = vec![format!("matrix.{case}")];
        let client = SdkClient::connect(config).await.unwrap();
        let response = client.request(operation, input).await.unwrap();
        assert_eq!(
            response.result(),
            Some(json!({"echo": format!("request-{case}")}))
        );
        client.close().await.unwrap();
        server.await.unwrap();
    }

    macro_rules! sdk_round_trip_matrix {
        ($($name:ident: $case:literal),+ $(,)?) => {
            $(
                #[tokio::test]
                async fn $name() {
                    round_trip_case($case).await;
                }
            )+
        };
    }

    sdk_round_trip_matrix! {
        protocol_case_000: 0, protocol_case_001: 1, protocol_case_002: 2, protocol_case_003: 3,
        protocol_case_004: 4, protocol_case_005: 5, protocol_case_006: 6, protocol_case_007: 7,
        protocol_case_008: 8, protocol_case_009: 9, protocol_case_010: 10, protocol_case_011: 11,
        protocol_case_012: 12, protocol_case_013: 13, protocol_case_014: 14, protocol_case_015: 15,
        protocol_case_016: 16, protocol_case_017: 17, protocol_case_018: 18, protocol_case_019: 19,
        protocol_case_020: 20, protocol_case_021: 21, protocol_case_022: 22, protocol_case_023: 23,
        protocol_case_024: 24, protocol_case_025: 25, protocol_case_026: 26, protocol_case_027: 27,
        protocol_case_028: 28, protocol_case_029: 29, protocol_case_030: 30, protocol_case_031: 31,
        protocol_case_032: 32, protocol_case_033: 33, protocol_case_034: 34, protocol_case_035: 35,
        protocol_case_036: 36, protocol_case_037: 37, protocol_case_038: 38, protocol_case_039: 39,
        protocol_case_040: 40, protocol_case_041: 41, protocol_case_042: 42, protocol_case_043: 43,
        protocol_case_044: 44, protocol_case_045: 45, protocol_case_046: 46, protocol_case_047: 47,
        protocol_case_048: 48, protocol_case_049: 49, protocol_case_050: 50, protocol_case_051: 51,
        protocol_case_052: 52, protocol_case_053: 53, protocol_case_054: 54, protocol_case_055: 55,
        protocol_case_056: 56, protocol_case_057: 57, protocol_case_058: 58, protocol_case_059: 59,
        protocol_case_060: 60, protocol_case_061: 61, protocol_case_062: 62, protocol_case_063: 63,
    }
}
