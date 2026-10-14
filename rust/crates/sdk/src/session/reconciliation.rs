//! Durable invocation state for recovering SDK sessions across process restarts.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::fs::File;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{SdkError, SdkResult};

static RECONCILIATION_WRITE_LOCK: Mutex<()> = Mutex::new(());

/// Lifecycle state persisted for an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationStatus {
    Accepted,
    Running,
    Uncertain,
    Terminal,
}

/// Result captured when an invocation reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationOutcome {
    Completed {
        message_count: usize,
    },
    Aborted {
        message_count: usize,
    },
    Failed {
        message: String,
        message_count: usize,
    },
}

/// Persisted lifecycle and reconciliation data for one invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationRecord {
    pub invocation_id: String,
    pub session_id: String,
    pub correlation_key: Option<String>,
    pub status: InvocationStatus,
    pub accepted_at_ms: u64,
    pub updated_at_ms: u64,
    pub outcome: Option<InvocationOutcome>,
    pub uncertainty_reason: Option<String>,
    pub terminal_event: Option<serde_json::Value>,
    pub terminal_scope_revision: Option<u64>,
}

/// File-backed store for invocation lifecycle reconciliation.
#[derive(Debug, Clone)]
pub struct InvocationReconciliationStore {
    root: PathBuf,
}

impl InvocationReconciliationStore {
    /// Creates a store handle without creating its root directory.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Inserts a new invocation record. Existing IDs are not overwritten.
    pub fn insert(&self, record: InvocationRecord) -> SdkResult<()> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.record_path(&record.invocation_id)?;
        if self.get(&record.invocation_id)?.is_some() {
            return Err(SdkError::invalid_input(format!(
                "invocation '{}' already exists",
                record.invocation_id
            )));
        }
        atomic_write_json(&path, &record)
    }

    /// Marks a non-terminal invocation as running.
    pub fn mark_running(&self, invocation_id: &str) -> SdkResult<()> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut record = self.required_record(invocation_id)?;
        if record.status != InvocationStatus::Accepted {
            return Err(SdkError::conflict(format!(
                "invocation '{}' cannot start from state {:?}",
                invocation_id, record.status
            )));
        }
        record.status = InvocationStatus::Running;
        record.uncertainty_reason = None;
        record.updated_at_ms = updated_at_ms(record.updated_at_ms);
        self.write_record(&record)
    }

    /// Marks a non-terminal invocation as uncertain with a recovery reason.
    pub fn mark_uncertain(&self, invocation_id: &str, reason: impl Into<String>) -> SdkResult<()> {
        let reason = reason.into();
        self.update_nonterminal(invocation_id, move |record| {
            record.status = InvocationStatus::Uncertain;
            record.uncertainty_reason = Some(reason);
        })
    }

    /// Records a terminal result. Once terminal, a record cannot be changed again.
    pub fn record_terminal(
        &self,
        invocation_id: &str,
        outcome: InvocationOutcome,
        terminal_event: Option<serde_json::Value>,
    ) -> SdkResult<()> {
        self.record_terminal_with_scope_revision(invocation_id, outcome, terminal_event, None)
    }

    /// Records a terminal result together with its durable correlation-scope revision.
    pub fn record_terminal_with_scope_revision(
        &self,
        invocation_id: &str,
        outcome: InvocationOutcome,
        terminal_event: Option<serde_json::Value>,
        terminal_scope_revision: Option<u64>,
    ) -> SdkResult<()> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut record = self.required_record(invocation_id)?;
        if record.status == InvocationStatus::Terminal {
            return Err(SdkError::invalid_input(format!(
                "invocation '{}' is already terminal",
                invocation_id
            )));
        }
        if record
            .outcome
            .as_ref()
            .is_some_and(|pending| pending != &outcome)
            || record
                .terminal_event
                .as_ref()
                .is_some_and(|pending| Some(pending) != terminal_event.as_ref())
        {
            return Err(SdkError::conflict(format!(
                "invocation '{}' has a different pending terminal result",
                invocation_id
            )));
        }
        record.status = InvocationStatus::Terminal;
        record.outcome = Some(outcome);
        record.uncertainty_reason = None;
        record.terminal_event = terminal_event;
        record.terminal_scope_revision = terminal_scope_revision;
        record.updated_at_ms = updated_at_ms(record.updated_at_ms);
        self.write_record(&record)
    }

    /// Persists a known turn result before its correlation scope is updated.
    ///
    /// The record remains non-terminal until the scope update succeeds, allowing
    /// startup or a later reconciliation read to safely finish the transition.
    pub fn record_pending_terminal(
        &self,
        invocation_id: &str,
        outcome: InvocationOutcome,
        terminal_event: serde_json::Value,
    ) -> SdkResult<()> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut record = self.required_record(invocation_id)?;
        if record.status == InvocationStatus::Terminal {
            return Err(SdkError::invalid_input(format!(
                "invocation '{}' is already terminal",
                invocation_id
            )));
        }
        if record.outcome.is_some() || record.terminal_event.is_some() {
            return Err(SdkError::conflict(format!(
                "invocation '{}' already has a pending terminal result",
                invocation_id
            )));
        }
        record.outcome = Some(outcome);
        record.terminal_event = Some(terminal_event);
        record.terminal_scope_revision = None;
        record.updated_at_ms = updated_at_ms(record.updated_at_ms);
        self.write_record(&record)
    }

    /// Loads one invocation record, if present.
    pub fn get(&self, invocation_id: &str) -> SdkResult<Option<InvocationRecord>> {
        let path = self.record_path(invocation_id)?;
        read_record(&path, invocation_id)
    }

    /// Loads all invocation records in stable invocation-ID order.
    pub fn list(&self) -> SdkResult<Vec<InvocationRecord>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(io_error(
                    "read invocation store directory",
                    &self.root,
                    error,
                ))
            }
        };

        let mut records = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|error| io_error("read invocation store entry", &self.root, error))?;
            if !entry
                .file_type()
                .map_err(|error| io_error("inspect invocation store entry", &entry.path(), error))?
                .is_file()
            {
                continue;
            }

            let path = entry.path();
            if path.extension() != Some(std::ffi::OsStr::new("json")) {
                continue;
            }
            let invocation_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| SdkError::invalid_input("invocation filename is not valid UTF-8"))?;
            validate_invocation_id(invocation_id)?;
            if let Some(record) = read_record(&path, invocation_id)? {
                records.push(record);
            }
        }
        records.sort_by(|left, right| left.invocation_id.cmp(&right.invocation_id));
        Ok(records)
    }

    /// Loads records associated with the exact correlation key.
    pub fn list_by_correlation_key(&self, key: &str) -> SdkResult<Vec<InvocationRecord>> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|record| record.correlation_key.as_deref() == Some(key))
            .collect())
    }

    /// Durably marks accepted and running records uncertain after a restart.
    ///
    /// Returns the records changed by this recovery pass; already uncertain and
    /// terminal records are left untouched and are not included in the result.
    pub fn reconcile_unfinished(&self) -> SdkResult<Vec<InvocationRecord>> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut recovered = Vec::new();
        for mut record in self.list()? {
            if matches!(
                record.status,
                InvocationStatus::Accepted | InvocationStatus::Running
            ) {
                record.status = InvocationStatus::Uncertain;
                record.uncertainty_reason =
                    Some("recovered unfinished invocation after restart".into());
                record.updated_at_ms = updated_at_ms(record.updated_at_ms);
                self.write_record(&record)?;
                recovered.push(record);
            }
        }
        Ok(recovered)
    }

    fn record_path(&self, invocation_id: &str) -> SdkResult<PathBuf> {
        validate_invocation_id(invocation_id)?;
        Ok(self.root.join(format!("{invocation_id}.json")))
    }

    fn required_record(&self, invocation_id: &str) -> SdkResult<InvocationRecord> {
        self.get(invocation_id)?.ok_or_else(|| {
            SdkError::invalid_input(format!("invocation '{invocation_id}' does not exist"))
        })
    }

    fn update_nonterminal(
        &self,
        invocation_id: &str,
        update: impl FnOnce(&mut InvocationRecord),
    ) -> SdkResult<()> {
        let _guard = RECONCILIATION_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut record = self.required_record(invocation_id)?;
        if record.status == InvocationStatus::Terminal {
            return Err(SdkError::invalid_input(format!(
                "invocation '{}' is already terminal",
                invocation_id
            )));
        }
        update(&mut record);
        record.updated_at_ms = updated_at_ms(record.updated_at_ms);
        self.write_record(&record)
    }

    fn write_record(&self, record: &InvocationRecord) -> SdkResult<()> {
        let path = self.record_path(&record.invocation_id)?;
        atomic_write_json(&path, record)
    }
}

/// Atomically serializes a value beside its target and renames it into place.
pub(super) fn atomic_write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> SdkResult<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| SdkError::internal(format!("serialize {}: {error}", path.display())))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| io_error("create store directory", parent, error))?;
    let filename = path
        .file_name()
        .ok_or_else(|| SdkError::invalid_input("store path has no filename"))?;
    let temporary_path = parent.join(format!(
        ".{}.{}.tmp",
        filename.to_string_lossy(),
        Uuid::new_v4()
    ));

    let write_result = (|| -> std::io::Result<()> {
        let mut temporary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)?;
        temporary.write_all(&bytes)?;
        temporary.sync_all()?;
        drop(temporary);
        fs::rename(&temporary_path, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();

    match write_result {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temporary_path);
            Err(io_error("atomically write JSON", path, error))
        }
    }
}

fn read_record(path: &Path, expected_id: &str) -> SdkResult<Option<InvocationRecord>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("read invocation record", path, error)),
    };
    let record: InvocationRecord = serde_json::from_slice(&bytes).map_err(|error| {
        SdkError::internal(format!(
            "parse invocation record {}: {error}",
            path.display()
        ))
    })?;
    validate_invocation_id(&record.invocation_id)?;
    if record.invocation_id != expected_id {
        return Err(SdkError::internal(format!(
            "invocation record ID '{}' does not match filename ID '{expected_id}'",
            record.invocation_id
        )));
    }
    Ok(Some(record))
}

fn validate_invocation_id(invocation_id: &str) -> SdkResult<()> {
    if invocation_id.is_empty()
        || invocation_id.len() > 200
        || !invocation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(SdkError::invalid_input(format!(
            "unsafe invocation ID '{invocation_id}'"
        )));
    }
    Ok(())
}

fn updated_at_ms(previous: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    now.max(previous.saturating_add(1))
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> SdkError {
    SdkError::internal(format!("{action} '{}': {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("sdk-session-{}", Uuid::new_v4()));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn accepted_record(invocation_id: &str) -> InvocationRecord {
        InvocationRecord {
            invocation_id: invocation_id.into(),
            session_id: "session-a".into(),
            correlation_key: Some("request-a".into()),
            status: InvocationStatus::Accepted,
            accepted_at_ms: 10,
            updated_at_ms: 10,
            outcome: None,
            uncertainty_reason: None,
            terminal_event: None,
            terminal_scope_revision: None,
        }
    }

    #[test]
    fn constructor_has_no_filesystem_side_effects() {
        let root = std::env::temp_dir().join(format!("sdk-session-{}", Uuid::new_v4()));
        let _store = InvocationReconciliationStore::new(&root);
        assert!(!root.exists());
    }

    #[test]
    fn invocation_records_survive_store_restart() {
        let root = TestDirectory::new();
        let record = accepted_record("invocation-a");
        InvocationReconciliationStore::new(&root.0)
            .insert(record.clone())
            .expect("insert record");

        let loaded = InvocationReconciliationStore::new(&root.0)
            .get("invocation-a")
            .expect("load record")
            .expect("record exists");
        assert_eq!(loaded, record);
    }

    #[test]
    fn running_transition_only_accepts_an_accepted_invocation() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        store
            .insert(accepted_record("invocation-a"))
            .expect("insert");
        store.mark_running("invocation-a").expect("mark running");
        assert_eq!(
            store.get("invocation-a").unwrap().unwrap().status,
            InvocationStatus::Running
        );
        assert!(store.mark_running("invocation-a").is_err());
        store
            .mark_uncertain("invocation-a", "recovered")
            .expect("mark uncertain");
        assert!(store.mark_running("invocation-a").is_err());
        assert_eq!(
            store.get("invocation-a").unwrap().unwrap().status,
            InvocationStatus::Uncertain
        );
    }

    #[test]
    fn uncertain_invocation_can_become_terminal_only_once() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        store
            .insert(accepted_record("invocation-a"))
            .expect("insert");
        store
            .mark_uncertain("invocation-a", "runtime disconnected")
            .expect("mark uncertain");
        store
            .record_terminal(
                "invocation-a",
                InvocationOutcome::Completed { message_count: 2 },
                Some(serde_json::json!({"type": "completed"})),
            )
            .expect("record terminal");

        let terminal = store.get("invocation-a").expect("load").expect("exists");
        assert_eq!(terminal.status, InvocationStatus::Terminal);
        assert_eq!(terminal.uncertainty_reason, None);
        assert_eq!(
            terminal.outcome,
            Some(InvocationOutcome::Completed { message_count: 2 })
        );
        assert!(store
            .record_terminal(
                "invocation-a",
                InvocationOutcome::Aborted { message_count: 0 },
                None,
            )
            .is_err());
        assert_eq!(store.get("invocation-a").expect("reload"), Some(terminal));
    }

    #[test]
    fn recovery_marks_only_accepted_and_running_records_uncertain() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        store
            .insert(accepted_record("accepted"))
            .expect("insert accepted");
        let mut running = accepted_record("running");
        running.status = InvocationStatus::Running;
        store.insert(running).expect("insert running");
        let mut terminal = accepted_record("terminal");
        terminal.status = InvocationStatus::Terminal;
        terminal.outcome = Some(InvocationOutcome::Aborted { message_count: 0 });
        store.insert(terminal.clone()).expect("insert terminal");

        let recovered = store.reconcile_unfinished().expect("reconcile");
        assert_eq!(recovered.len(), 2);
        assert!(recovered
            .iter()
            .all(|record| record.status == InvocationStatus::Uncertain));
        assert_eq!(
            store.get("terminal").expect("load terminal"),
            Some(terminal)
        );
        assert!(store
            .reconcile_unfinished()
            .expect("repeat recovery")
            .is_empty());
    }

    #[test]
    fn pending_terminal_result_survives_uncertainty_and_can_be_finalized() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        store
            .insert(accepted_record("pending"))
            .expect("insert accepted record");
        store.mark_running("pending").expect("mark running");
        store
            .record_pending_terminal(
                "pending",
                InvocationOutcome::Completed { message_count: 3 },
                serde_json::json!({"eventType": "turn_ended"}),
            )
            .expect("record pending result");
        store
            .mark_uncertain("pending", "terminal scope store was unavailable")
            .expect("mark uncertain");

        let pending = store.get("pending").unwrap().unwrap();
        assert_eq!(pending.status, InvocationStatus::Uncertain);
        assert_eq!(
            pending.outcome,
            Some(InvocationOutcome::Completed { message_count: 3 })
        );
        store
            .record_terminal_with_scope_revision(
                "pending",
                pending.outcome.unwrap(),
                pending.terminal_event,
                Some(1),
            )
            .expect("finalize after recovery");
        assert_eq!(
            store.get("pending").unwrap().unwrap().status,
            InvocationStatus::Terminal
        );
    }

    #[test]
    fn concurrent_terminal_and_uncertain_transitions_preserve_terminal_result() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        store
            .insert(accepted_record("concurrent"))
            .expect("insert accepted record");
        store.mark_running("concurrent").expect("mark running");

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let terminal_store = store.clone();
        let terminal_barrier = std::sync::Arc::clone(&barrier);
        let terminal = std::thread::spawn(move || {
            terminal_barrier.wait();
            terminal_store.record_terminal(
                "concurrent",
                InvocationOutcome::Completed { message_count: 1 },
                None,
            )
        });
        let uncertain_store = store.clone();
        let uncertain_barrier = std::sync::Arc::clone(&barrier);
        let uncertain = std::thread::spawn(move || {
            uncertain_barrier.wait();
            uncertain_store.mark_uncertain("concurrent", "raced with terminal write")
        });
        barrier.wait();
        let _ = terminal.join().expect("terminal writer thread");
        let _ = uncertain.join().expect("uncertain writer thread");

        let record = store.get("concurrent").unwrap().unwrap();
        assert_eq!(record.status, InvocationStatus::Terminal);
        assert_eq!(
            record.outcome,
            Some(InvocationOutcome::Completed { message_count: 1 })
        );
    }

    #[test]
    fn concurrent_duplicate_inserts_have_only_one_winner() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let first_store = store.clone();
        let first_barrier = std::sync::Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_store.insert(accepted_record("duplicate"))
        });
        let second_store = store.clone();
        let second_barrier = std::sync::Arc::clone(&barrier);
        let second = std::thread::spawn(move || {
            second_barrier.wait();
            second_store.insert(accepted_record("duplicate"))
        });
        barrier.wait();
        let first = first.join().expect("first insert thread");
        let second = second.join().expect("second insert thread");

        assert_ne!(first.is_ok(), second.is_ok());
        assert_eq!(
            store.get("duplicate").unwrap().unwrap().invocation_id,
            "duplicate"
        );
    }

    #[test]
    fn malformed_ids_and_json_are_rejected() {
        let root = TestDirectory::new();
        let store = InvocationReconciliationStore::new(&root.0);
        assert!(store.get("../escape").is_err());
        assert!(store.insert(accepted_record("bad/id")).is_err());

        fs::write(root.0.join("malformed.json"), "{").expect("write malformed record");
        assert!(store.get("malformed").is_err());
        assert!(store.list().is_err());
    }
}
