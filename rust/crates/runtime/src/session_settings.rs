use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::json::JsonValue;

/// Classification of a setting by its category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SettingCategory {
    Model,
    Behavior,
    Ux,
    Advanced,
}

impl Display for SettingCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Model => write!(f, "model"),
            Self::Behavior => write!(f, "behavior"),
            Self::Ux => write!(f, "ux"),
            Self::Advanced => write!(f, "advanced"),
        }
    }
}

/// Type of a setting value for schema validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingType {
    String,
    Number,
    Boolean,
    Object,
}

impl Display for SettingType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::Number => write!(f, "number"),
            Self::Boolean => write!(f, "boolean"),
            Self::Object => write!(f, "object"),
        }
    }
}

/// Schema definition for a single overridable setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingSchema {
    /// Unique identifier for this setting
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Expected type of the value
    pub value_type: SettingType,
    /// Setting category (model, behavior, UX, advanced)
    pub category: SettingCategory,
    /// Whether this setting can be overridden at runtime
    pub overridable: bool,
    /// Default value if not specified
    pub default_value: Option<JsonValue>,
    /// Allowed values (for string/enum settings), if restricted
    pub allowed_values: Option<Vec<JsonValue>>,
}

impl SettingSchema {
    /// Create a new setting schema with minimal required fields.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        value_type: SettingType,
        category: SettingCategory,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            value_type,
            category,
            overridable: true,
            default_value: None,
            allowed_values: None,
        }
    }

    /// Set the default value for this setting.
    pub fn with_default(mut self, default: JsonValue) -> Self {
        self.default_value = Some(default);
        self
    }

    /// Set whether this setting can be overridden.
    pub fn with_overridable(mut self, overridable: bool) -> Self {
        self.overridable = overridable;
        self
    }

    /// Set allowed values for this setting (for enum-like settings).
    pub fn with_allowed_values(mut self, allowed: Vec<JsonValue>) -> Self {
        self.allowed_values = Some(allowed);
        self
    }

    /// Validate a value against this schema.
    pub fn validate(&self, value: &JsonValue) -> Result<(), String> {
        // Type check
        match (self.value_type, value) {
            (SettingType::String, JsonValue::String(_)) => {}
            (SettingType::Number, JsonValue::Number(_)) => {}
            (SettingType::Boolean, JsonValue::Bool(_)) => {}
            (SettingType::Object, JsonValue::Object(_)) => {}
            _ => {
                return Err(format!(
                    "Setting '{}' expects {}, got {}",
                    self.name,
                    self.value_type,
                    value_type_label(value)
                ))
            }
        }

        // Allowed values check
        if let Some(ref allowed) = self.allowed_values {
            if !allowed.contains(value) {
                return Err(format!(
                    "Setting '{}' value not in allowed list: {:?}",
                    self.name, allowed
                ));
            }
        }

        Ok(())
    }
}

/// Settings that can be overridden at the session level.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionSettings {
    /// Global settings (from runtime config)
    pub global: BTreeMap<String, JsonValue>,
    /// Session-specific overrides
    pub session_overrides: BTreeMap<String, JsonValue>,
    /// Per-request overrides
    pub request_overrides: BTreeMap<String, JsonValue>,
}

impl SessionSettings {
    /// Create a new session settings container.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a global setting.
    pub fn set_global(&mut self, name: impl Into<String>, value: JsonValue) {
        self.global.insert(name.into(), value);
    }

    /// Set a session-level override.
    pub fn set_session_override(&mut self, name: impl Into<String>, value: JsonValue) {
        self.session_overrides.insert(name.into(), value);
    }

    /// Set a per-request override.
    pub fn set_request_override(&mut self, name: impl Into<String>, value: JsonValue) {
        self.request_overrides.insert(name.into(), value);
    }

    /// Get effective value for a setting, applying the override hierarchy:
    /// request > session > global > default
    pub fn get_effective(&self, name: &str, schema: &SettingSchema) -> Option<JsonValue> {
        if let Some(value) = self.request_overrides.get(name) {
            return Some(value.clone());
        }
        if let Some(value) = self.session_overrides.get(name) {
            return Some(value.clone());
        }
        if let Some(value) = self.global.get(name) {
            return Some(value.clone());
        }
        schema.default_value.clone()
    }

    /// Clear all request-level overrides.
    pub fn clear_request_overrides(&mut self) {
        self.request_overrides.clear();
    }
}

/// Registry of all overridable settings with their schemas.
#[derive(Debug, Clone)]
pub struct SettingSchemaRegistry {
    schemas: BTreeMap<String, SettingSchema>,
}

impl SettingSchemaRegistry {
    /// Create a new settings schema registry.
    pub fn new() -> Self {
        Self {
            schemas: BTreeMap::new(),
        }
    }

    /// Register a setting schema.
    pub fn register(&mut self, schema: SettingSchema) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    /// Get a schema by name.
    pub fn get(&self, name: &str) -> Option<&SettingSchema> {
        self.schemas.get(name)
    }

    /// Get all schemas.
    pub fn all(&self) -> impl Iterator<Item = &SettingSchema> {
        self.schemas.values()
    }

    /// Get schemas for a specific category.
    pub fn by_category(&self, category: SettingCategory) -> Vec<&SettingSchema> {
        self.schemas
            .values()
            .filter(|s| s.category == category)
            .collect()
    }

    /// Validate a settings object against the registry.
    pub fn validate(&self, settings: &BTreeMap<String, JsonValue>) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        for (name, value) in settings {
            if let Some(schema) = self.get(name) {
                if !schema.overridable {
                    errors.push(format!("Setting '{}' is not overridable", name));
                } else if let Err(e) = schema.validate(value) {
                    errors.push(e);
                }
            } else {
                errors.push(format!("Unknown setting: '{}'", name));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Count total number of registered settings.
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

impl Default for SettingSchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Applies and merges settings from multiple sources.
pub struct SettingApplicator {
    registry: Arc<Mutex<SettingSchemaRegistry>>,
    settings: Arc<Mutex<SessionSettings>>,
}

impl SettingApplicator {
    /// Create a new setting applicator.
    pub fn new(registry: SettingSchemaRegistry) -> Self {
        Self {
            registry: Arc::new(Mutex::new(registry)),
            settings: Arc::new(Mutex::new(SessionSettings::new())),
        }
    }

    /// Apply global settings.
    pub fn apply_global(&self, settings: BTreeMap<String, JsonValue>) -> Result<(), Vec<String>> {
        let registry = self.registry.lock().unwrap();
        registry.validate(&settings)?;
        let mut s = self.settings.lock().unwrap();
        s.global = settings;
        Ok(())
    }

    /// Apply session-level overrides.
    pub fn apply_session_overrides(
        &self,
        overrides: BTreeMap<String, JsonValue>,
    ) -> Result<(), Vec<String>> {
        let registry = self.registry.lock().unwrap();
        registry.validate(&overrides)?;
        let mut s = self.settings.lock().unwrap();
        s.session_overrides = overrides;
        Ok(())
    }

    /// Apply request-level overrides.
    pub fn apply_request_overrides(
        &self,
        overrides: BTreeMap<String, JsonValue>,
    ) -> Result<(), Vec<String>> {
        let registry = self.registry.lock().unwrap();
        registry.validate(&overrides)?;
        let mut s = self.settings.lock().unwrap();
        s.request_overrides = overrides;
        Ok(())
    }

    /// Get the effective value for a setting.
    pub fn get_effective(&self, name: &str) -> Option<JsonValue> {
        let registry = self.registry.lock().unwrap();
        if let Some(schema) = registry.get(name) {
            let settings = self.settings.lock().unwrap();
            settings.get_effective(name, schema)
        } else {
            None
        }
    }

    /// Get all effective settings.
    pub fn get_all_effective(&self) -> BTreeMap<String, JsonValue> {
        let mut result = BTreeMap::new();
        let registry = self.registry.lock().unwrap();
        let settings = self.settings.lock().unwrap();

        for schema in registry.all() {
            if let Some(value) = settings.get_effective(&schema.name, schema) {
                result.insert(schema.name.clone(), value);
            }
        }

        result
    }

    /// Clear all request-level overrides.
    pub fn clear_request_overrides(&self) {
        let mut s = self.settings.lock().unwrap();
        s.clear_request_overrides();
    }

    /// Get a clone of the current settings.
    pub fn get_settings(&self) -> SessionSettings {
        self.settings.lock().unwrap().clone()
    }
}

/// Configuration hotreload monitor that watches for file changes.
pub struct ConfigHotreloadMonitor {
    watched_file: PathBuf,
    last_modified: Arc<Mutex<Option<SystemTime>>>,
}

impl ConfigHotreloadMonitor {
    /// Create a new hotreload monitor for a config file.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let last_modified = fs::metadata(&path).ok().and_then(|m| m.modified().ok());

        Self {
            watched_file: path,
            last_modified: Arc::new(Mutex::new(last_modified)),
        }
    }

    /// Check if the watched file has been modified.
    pub fn has_changed(&self) -> bool {
        if let Ok(metadata) = fs::metadata(&self.watched_file) {
            if let Ok(modified) = metadata.modified() {
                let mut last = self.last_modified.lock().unwrap();
                if let Some(prev_modified) = *last {
                    if modified > prev_modified {
                        *last = Some(modified);
                        return true;
                    }
                }
                *last = Some(modified);
            }
        }
        false
    }

    /// Get the modification time of the watched file.
    pub fn get_modified_time(&self) -> Option<u64> {
        fs::metadata(&self.watched_file)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
    }
}

fn value_type_label(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::String(_) => "string",
        JsonValue::Number(_) => "number",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
        JsonValue::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_schema_creation() {
        let schema = SettingSchema::new(
            "test_setting",
            "A test setting",
            SettingType::String,
            SettingCategory::Model,
        );

        assert_eq!(schema.name, "test_setting");
        assert_eq!(schema.value_type, SettingType::String);
        assert_eq!(schema.category, SettingCategory::Model);
        assert!(schema.overridable);
    }

    #[test]
    fn setting_schema_validation() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        );

        let valid = JsonValue::String("claude-3-opus".to_string());
        assert!(schema.validate(&valid).is_ok());

        let invalid = JsonValue::Number(42);
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn setting_schema_allowed_values() {
        let schema = SettingSchema::new(
            "mode",
            "Permission mode",
            SettingType::String,
            SettingCategory::Behavior,
        )
        .with_allowed_values(vec![
            JsonValue::String("read_only".to_string()),
            JsonValue::String("danger_full_access".to_string()),
        ]);

        let valid = JsonValue::String("read_only".to_string());
        assert!(schema.validate(&valid).is_ok());

        let invalid = JsonValue::String("invalid_mode".to_string());
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn session_settings_hierarchy() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        )
        .with_default(JsonValue::String("claude-3-haiku".to_string()));

        let mut settings = SessionSettings::new();
        settings.set_global("model", JsonValue::String("claude-3-sonnet".to_string()));
        settings.set_session_override("model", JsonValue::String("claude-3-opus".to_string()));

        let effective = settings.get_effective("model", &schema);
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-opus".to_string()))
        );

        settings.clear_request_overrides();
        let effective = settings.get_effective("model", &schema);
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-opus".to_string()))
        );
    }

    #[test]
    fn session_settings_request_override_precedence() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        )
        .with_default(JsonValue::String("claude-3-haiku".to_string()));

        let mut settings = SessionSettings::new();
        settings.set_global("model", JsonValue::String("claude-3-sonnet".to_string()));
        settings.set_session_override("model", JsonValue::String("claude-3-opus".to_string()));
        settings.set_request_override("model", JsonValue::String("claude-3-opus-200k".to_string()));

        let effective = settings.get_effective("model", &schema);
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-opus-200k".to_string()))
        );
    }

    #[test]
    fn registry_registration_and_retrieval() {
        let mut registry = SettingSchemaRegistry::new();

        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        );

        registry.register(schema.clone());

        assert_eq!(registry.get("model"), Some(&schema));
        assert!(registry.get("nonexistent").is_none());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_validation() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let mut settings = BTreeMap::new();
        settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );

        assert!(registry.validate(&settings).is_ok());
    }

    #[test]
    fn registry_validation_invalid_type() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let mut settings = BTreeMap::new();
        settings.insert("model".to_string(), JsonValue::Number(42));

        assert!(registry.validate(&settings).is_err());
    }

    #[test]
    fn registry_validation_unknown_setting() {
        let registry = SettingSchemaRegistry::new();

        let mut settings = BTreeMap::new();
        settings.insert(
            "unknown".to_string(),
            JsonValue::String("value".to_string()),
        );

        assert!(registry.validate(&settings).is_err());
    }

    #[test]
    fn setting_applicator_apply_global() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut settings = BTreeMap::new();
        settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );

        assert!(applicator.apply_global(settings).is_ok());

        let effective = applicator.get_effective("model");
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-opus".to_string()))
        );
    }

    #[test]
    fn setting_applicator_override_hierarchy() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        applicator.apply_global(global_settings).unwrap();

        let mut session_overrides = BTreeMap::new();
        session_overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );
        applicator
            .apply_session_overrides(session_overrides)
            .unwrap();

        let effective = applicator.get_effective("model");
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-opus".to_string()))
        );
    }

    #[test]
    fn setting_applicator_clear_request_overrides() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        applicator.apply_global(global_settings).unwrap();

        let mut request_overrides = BTreeMap::new();
        request_overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );
        applicator
            .apply_request_overrides(request_overrides)
            .unwrap();

        applicator.clear_request_overrides();

        let effective = applicator.get_effective("model");
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-sonnet".to_string()))
        );
    }

    #[test]
    fn registry_by_category() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));
        registry.register(SettingSchema::new(
            "temperature",
            "Sampling temperature",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        let model_settings = registry.by_category(SettingCategory::Model);
        assert_eq!(model_settings.len(), 1);

        let advanced_settings = registry.by_category(SettingCategory::Advanced);
        assert_eq!(advanced_settings.len(), 1);
    }

    #[test]
    fn hotreload_monitor_creation() {
        // Create a temporary file for testing
        let temp_file = std::env::temp_dir().join("test_config.json");
        fs::write(&temp_file, "{}").unwrap();

        let monitor = ConfigHotreloadMonitor::new(&temp_file);
        assert!(monitor.get_modified_time().is_some());

        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn permission_mode_override_per_session() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(
            SettingSchema::new(
                "permission_mode",
                "Permission mode for the session",
                SettingType::String,
                SettingCategory::Behavior,
            )
            .with_allowed_values(vec![
                JsonValue::String("read_only".to_string()),
                JsonValue::String("workspace_write".to_string()),
                JsonValue::String("danger_full_access".to_string()),
            ]),
        );

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "permission_mode".to_string(),
            JsonValue::String("read_only".to_string()),
        );
        assert!(applicator.apply_global(global_settings).is_ok());

        let mut session_overrides = BTreeMap::new();
        session_overrides.insert(
            "permission_mode".to_string(),
            JsonValue::String("workspace_write".to_string()),
        );
        assert!(applicator
            .apply_session_overrides(session_overrides)
            .is_ok());

        let effective = applicator.get_effective("permission_mode");
        assert_eq!(
            effective,
            Some(JsonValue::String("workspace_write".to_string()))
        );
    }

    #[test]
    fn temperature_token_overrides_work() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "temperature",
            "Sampling temperature",
            SettingType::Number,
            SettingCategory::Advanced,
        ));
        registry.register(SettingSchema::new(
            "max_tokens",
            "Maximum tokens to generate",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert("temperature".to_string(), JsonValue::Number(7));
        global_settings.insert("max_tokens".to_string(), JsonValue::Number(4096));
        assert!(applicator.apply_global(global_settings).is_ok());

        let mut session_overrides = BTreeMap::new();
        session_overrides.insert("temperature".to_string(), JsonValue::Number(5));
        assert!(applicator
            .apply_session_overrides(session_overrides)
            .is_ok());

        assert_eq!(
            applicator.get_effective("temperature"),
            Some(JsonValue::Number(5))
        );
        assert_eq!(
            applicator.get_effective("max_tokens"),
            Some(JsonValue::Number(4096))
        );
    }

    #[test]
    fn setting_propagation_to_all_tools() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));
        registry.register(SettingSchema::new(
            "context_window",
            "Context window size",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        global_settings.insert("context_window".to_string(), JsonValue::Number(200000));
        assert!(applicator.apply_global(global_settings).is_ok());

        let all_effective = applicator.get_all_effective();
        assert_eq!(all_effective.len(), 2);
        assert_eq!(
            all_effective.get("model"),
            Some(&JsonValue::String("claude-3-sonnet".to_string()))
        );
        assert_eq!(
            all_effective.get("context_window"),
            Some(&JsonValue::Number(200000))
        );
    }

    #[test]
    fn setting_default_values() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        )
        .with_default(JsonValue::String("claude-3-haiku".to_string()));

        let settings = SessionSettings::new();
        let effective = settings.get_effective("model", &schema);
        assert_eq!(
            effective,
            Some(JsonValue::String("claude-3-haiku".to_string()))
        );
    }

    #[test]
    fn setting_non_overridable() {
        let mut registry = SettingSchemaRegistry::new();
        let schema = SettingSchema::new(
            "system_prompt",
            "System prompt",
            SettingType::String,
            SettingCategory::Advanced,
        )
        .with_overridable(false);

        registry.register(schema);

        let mut settings = BTreeMap::new();
        settings.insert(
            "system_prompt".to_string(),
            JsonValue::String("custom prompt".to_string()),
        );

        assert!(registry.validate(&settings).is_err());
    }

    #[test]
    fn applicator_validation_error_on_invalid_override() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut invalid_overrides = BTreeMap::new();
        invalid_overrides.insert("model".to_string(), JsonValue::Number(42));

        let result = applicator.apply_session_overrides(invalid_overrides);
        assert!(result.is_err());
    }

    #[test]
    fn registry_len_and_is_empty() {
        let mut registry = SettingSchemaRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn dynamic_setting_application_mid_session() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        // Apply initial settings
        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        applicator.apply_global(global_settings).unwrap();

        // Override mid-session
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );
        applicator.apply_request_overrides(overrides).unwrap();

        assert_eq!(
            applicator.get_effective("model"),
            Some(JsonValue::String("claude-3-opus".to_string()))
        );

        // Change override again
        let mut new_overrides = BTreeMap::new();
        new_overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-haiku".to_string()),
        );
        applicator.apply_request_overrides(new_overrides).unwrap();

        assert_eq!(
            applicator.get_effective("model"),
            Some(JsonValue::String("claude-3-haiku".to_string()))
        );
    }

    #[test]
    fn config_hotreload_has_changed() {
        let temp_file = std::env::temp_dir().join("test_hotreload.json");
        fs::write(&temp_file, r#"{"model":"claude-3-sonnet"}"#).unwrap();

        let monitor = ConfigHotreloadMonitor::new(&temp_file);

        // First check should not detect change
        assert!(!monitor.has_changed());

        // Modify the file
        std::thread::sleep(std::time::Duration::from_millis(100));
        fs::write(&temp_file, r#"{"model":"claude-3-opus"}"#).unwrap();

        // Second check should detect change
        assert!(monitor.has_changed());

        // Third check should not detect change (already recorded)
        assert!(!monitor.has_changed());

        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn merge_global_session_request_hierarchy() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(
            SettingSchema::new(
                "model",
                "The model to use",
                SettingType::String,
                SettingCategory::Model,
            )
            .with_default(JsonValue::String("claude-3-haiku".to_string())),
        );
        registry.register(SettingSchema::new(
            "temperature",
            "Sampling temperature",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        global_settings.insert("temperature".to_string(), JsonValue::Number(8));
        applicator.apply_global(global_settings).unwrap();

        let mut session_overrides = BTreeMap::new();
        session_overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );
        applicator
            .apply_session_overrides(session_overrides)
            .unwrap();

        let mut request_overrides = BTreeMap::new();
        request_overrides.insert("temperature".to_string(), JsonValue::Number(6));
        applicator
            .apply_request_overrides(request_overrides)
            .unwrap();

        // model should use session override
        assert_eq!(
            applicator.get_effective("model"),
            Some(JsonValue::String("claude-3-opus".to_string()))
        );

        // temperature should use request override
        assert_eq!(
            applicator.get_effective("temperature"),
            Some(JsonValue::Number(6))
        );
    }

    #[test]
    fn setting_schema_with_builder_pattern() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        )
        .with_default(JsonValue::String("claude-3-sonnet".to_string()))
        .with_overridable(true)
        .with_allowed_values(vec![
            JsonValue::String("claude-3-haiku".to_string()),
            JsonValue::String("claude-3-sonnet".to_string()),
            JsonValue::String("claude-3-opus".to_string()),
        ]);

        assert!(schema
            .validate(&JsonValue::String("claude-3-opus".to_string()))
            .is_ok());
        assert!(schema
            .validate(&JsonValue::String("invalid-model".to_string()))
            .is_err());
    }

    #[test]
    fn get_settings_clone() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut global_settings = BTreeMap::new();
        global_settings.insert(
            "model".to_string(),
            JsonValue::String("claude-3-opus".to_string()),
        );
        applicator.apply_global(global_settings).unwrap();

        let mut session_overrides = BTreeMap::new();
        session_overrides.insert(
            "model".to_string(),
            JsonValue::String("claude-3-sonnet".to_string()),
        );
        applicator
            .apply_session_overrides(session_overrides)
            .unwrap();

        let settings = applicator.get_settings();
        assert_eq!(settings.global.len(), 1);
        assert_eq!(settings.session_overrides.len(), 1);
    }

    #[test]
    fn multiple_settings_per_category() {
        let mut registry = SettingSchemaRegistry::new();

        // Model settings
        registry.register(SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        ));

        // Behavior settings
        registry.register(SettingSchema::new(
            "permission_mode",
            "Permission mode",
            SettingType::String,
            SettingCategory::Behavior,
        ));
        registry.register(SettingSchema::new(
            "auto_save",
            "Enable auto-save",
            SettingType::Boolean,
            SettingCategory::Behavior,
        ));

        // UX settings
        registry.register(SettingSchema::new(
            "verbose_output",
            "Enable verbose output",
            SettingType::Boolean,
            SettingCategory::Ux,
        ));

        // Advanced settings
        registry.register(SettingSchema::new(
            "temperature",
            "Sampling temperature",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        assert_eq!(registry.by_category(SettingCategory::Model).len(), 1);
        assert_eq!(registry.by_category(SettingCategory::Behavior).len(), 2);
        assert_eq!(registry.by_category(SettingCategory::Ux).len(), 1);
        assert_eq!(registry.by_category(SettingCategory::Advanced).len(), 1);
        assert_eq!(registry.len(), 5);
    }

    #[test]
    fn setting_with_no_default() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        );

        let settings = SessionSettings::new();
        let effective = settings.get_effective("model", &schema);
        assert_eq!(effective, None);
    }

    #[test]
    fn setting_category_display() {
        assert_eq!(SettingCategory::Model.to_string(), "model");
        assert_eq!(SettingCategory::Behavior.to_string(), "behavior");
        assert_eq!(SettingCategory::Ux.to_string(), "ux");
        assert_eq!(SettingCategory::Advanced.to_string(), "advanced");
    }

    #[test]
    fn setting_type_display() {
        assert_eq!(SettingType::String.to_string(), "string");
        assert_eq!(SettingType::Number.to_string(), "number");
        assert_eq!(SettingType::Boolean.to_string(), "boolean");
        assert_eq!(SettingType::Object.to_string(), "object");
    }

    #[test]
    fn setting_validation_error_message() {
        let schema = SettingSchema::new(
            "model",
            "The model to use",
            SettingType::String,
            SettingCategory::Model,
        );

        let invalid = JsonValue::Number(42);
        let result = schema.validate(&invalid);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("model"));
        assert!(err.contains("string"));
    }

    #[test]
    fn allowed_values_validation_error() {
        let schema = SettingSchema::new(
            "mode",
            "Permission mode",
            SettingType::String,
            SettingCategory::Behavior,
        )
        .with_allowed_values(vec![
            JsonValue::String("read_only".to_string()),
            JsonValue::String("danger_full_access".to_string()),
        ]);

        let invalid = JsonValue::String("invalid_mode".to_string());
        let result = schema.validate(&invalid);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("mode"));
    }

    #[test]
    fn session_settings_clear_preserves_global_and_session() {
        let mut settings = SessionSettings::new();
        settings.set_global("model", JsonValue::String("sonnet".to_string()));
        settings.set_session_override("model", JsonValue::String("opus".to_string()));
        settings.set_request_override("model", JsonValue::String("haiku".to_string()));

        settings.clear_request_overrides();

        assert_eq!(settings.global.len(), 1);
        assert_eq!(settings.session_overrides.len(), 1);
        assert_eq!(settings.request_overrides.len(), 0);
    }

    #[test]
    fn object_type_setting() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "custom_config",
            "Custom configuration object",
            SettingType::Object,
            SettingCategory::Advanced,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut config_obj = BTreeMap::new();
        config_obj.insert("nested".to_string(), JsonValue::String("value".to_string()));

        let mut settings = BTreeMap::new();
        settings.insert("custom_config".to_string(), JsonValue::Object(config_obj));

        assert!(applicator.apply_global(settings).is_ok());
        assert!(applicator.get_effective("custom_config").is_some());
    }

    #[test]
    fn boolean_type_setting() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "verbose",
            "Enable verbose output",
            SettingType::Boolean,
            SettingCategory::Ux,
        ));

        let applicator = SettingApplicator::new(registry);

        let mut settings = BTreeMap::new();
        settings.insert("verbose".to_string(), JsonValue::Bool(true));

        assert!(applicator.apply_global(settings).is_ok());
        assert_eq!(
            applicator.get_effective("verbose"),
            Some(JsonValue::Bool(true))
        );
    }

    #[test]
    fn registry_all_returns_all_schemas() {
        let mut registry = SettingSchemaRegistry::new();
        registry.register(SettingSchema::new(
            "model",
            "The model",
            SettingType::String,
            SettingCategory::Model,
        ));
        registry.register(SettingSchema::new(
            "temperature",
            "Temperature",
            SettingType::Number,
            SettingCategory::Advanced,
        ));

        let all: Vec<_> = registry.all().collect();
        assert_eq!(all.len(), 2);
    }
}
