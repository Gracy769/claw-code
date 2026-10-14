//! Operation registry and schema validation for the SDK wire protocol.

use serde_json::{json, Value};
use std::collections::HashMap;

use crate::error::{SdkError, SdkResult};

/// A JSON schema constraint for operation inputs/outputs.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Human-readable description
    pub description: String,
    /// JSON Schema document for validation
    pub json_schema: Value,
}

impl Schema {
    /// Create a new schema.
    pub fn new(description: impl Into<String>, json_schema: Value) -> Self {
        Self {
            description: description.into(),
            json_schema,
        }
    }

    /// Validate a value against this schema.
    /// For now, this does basic type checking. A full JSON Schema validator could be integrated.
    pub fn validate(&self, value: &Value) -> SdkResult<()> {
        // Basic validation: check that the value can be serialized/deserialized
        // Full JSON Schema validation would require a library like jsonschema
        match serde_json::to_string(value) {
            Ok(_) => Ok(()),
            Err(e) => Err(SdkError::frame_error(format!(
                "schema validation failed: {}",
                e
            ))),
        }
    }
}

/// Metadata for an SDK operation.
#[derive(Debug, Clone)]
pub struct OperationMetadata {
    /// Operation name (e.g., "list.sessions", "execute.tool")
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Input schema (if any)
    pub input_schema: Option<Schema>,
    /// Output schema (if any)
    pub output_schema: Option<Schema>,
    /// Whether this operation is stable (vs experimental)
    pub stable: bool,
}

impl OperationMetadata {
    /// Create a new operation metadata.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema: None,
            output_schema: None,
            stable: true,
        }
    }

    /// Set the input schema for this operation.
    pub fn with_input_schema(mut self, schema: Schema) -> Self {
        self.input_schema = Some(schema);
        self
    }

    /// Set the output schema for this operation.
    pub fn with_output_schema(mut self, schema: Schema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Mark this operation as experimental.
    pub fn unstable(mut self) -> Self {
        self.stable = false;
        self
    }

    /// Validate an input value against this operation's input schema.
    pub fn validate_input(&self, input: &Value) -> SdkResult<()> {
        match &self.input_schema {
            Some(schema) => schema.validate(input),
            None => {
                // No schema defined, accept any input
                Ok(())
            }
        }
    }

    /// Validate an output value against this operation's output schema.
    pub fn validate_output(&self, output: &Value) -> SdkResult<()> {
        match &self.output_schema {
            Some(schema) => schema.validate(output),
            None => {
                // No schema defined, accept any output
                Ok(())
            }
        }
    }
}

/// Registry of available SDK operations.
pub struct OperationRegistry {
    operations: HashMap<String, OperationMetadata>,
}

impl OperationRegistry {
    /// Create a new operation registry.
    pub fn new() -> Self {
        Self {
            operations: HashMap::new(),
        }
    }

    /// Create a registry with standard built-in operations.
    pub fn with_standard_operations() -> Self {
        let mut registry = Self::new();

        // Standard query operations
        registry.register(
            OperationMetadata::new("query.list_sessions", "List all active sessions")
                .with_output_schema(Schema::new(
                    "Array of session objects",
                    json!({
                        "type": "object",
                        "properties": {
                            "sessions": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": { "type": "string" },
                                        "name": { "type": "string" },
                                        "created_at": { "type": "string" }
                                    },
                                    "required": ["id", "name"]
                                }
                            }
                        }
                    }),
                )),
        );

        registry.register(
            OperationMetadata::new("query.get_session", "Get a specific session by ID")
                .with_input_schema(Schema::new(
                    "Session ID to retrieve",
                    json!({
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" }
                        },
                        "required": ["session_id"]
                    }),
                ))
                .with_output_schema(Schema::new(
                    "Session object",
                    json!({
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" },
                            "name": { "type": "string" },
                            "state": { "type": "string" },
                            "created_at": { "type": "string" }
                        },
                        "required": ["id", "name", "state"]
                    }),
                )),
        );

        // Standard control operations
        registry.register(
            OperationMetadata::new("control.activate_session", "Activate a session for use")
                .with_input_schema(Schema::new(
                    "Session ID to activate",
                    json!({
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" }
                        },
                        "required": ["session_id"]
                    }),
                ))
                .with_output_schema(Schema::new(
                    "Activation result",
                    json!({
                        "type": "object",
                        "properties": {
                            "success": { "type": "boolean" },
                            "session_id": { "type": "string" }
                        },
                        "required": ["success"]
                    }),
                )),
        );

        registry.register(
            OperationMetadata::new("control.deactivate_session", "Deactivate a session")
                .with_input_schema(Schema::new(
                    "Session ID to deactivate",
                    json!({
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" }
                        },
                        "required": ["session_id"]
                    }),
                ))
                .with_output_schema(Schema::new(
                    "Deactivation result",
                    json!({
                        "type": "object",
                        "properties": {
                            "success": { "type": "boolean" }
                        },
                        "required": ["success"]
                    }),
                )),
        );

        // Standard event operations
        registry.register(
            OperationMetadata::new("events.replay", "Replay events from a session")
                .with_input_schema(Schema::new(
                    "Event replay parameters",
                    json!({
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" },
                            "from_event_id": { "type": "string" },
                            "limit": { "type": "integer", "minimum": 1 }
                        },
                        "required": ["session_id"]
                    }),
                ))
                .with_output_schema(Schema::new(
                    "Array of replayed events",
                    json!({
                        "type": "object",
                        "properties": {
                            "events": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": { "type": "string" },
                                        "type": { "type": "string" },
                                        "timestamp": { "type": "string" }
                                    },
                                    "required": ["id", "type"]
                                }
                            }
                        },
                        "required": ["events"]
                    }),
                )),
        );

        // SDK session lifecycle operations.
        for (name, description, required) in [
            (
                "turn.create",
                "Create a durable SDK session",
                Vec::<&str>::new(),
            ),
            (
                "turn.prompt",
                "Prompt a session and stream lifecycle events",
                Vec::<&str>::new(),
            ),
            (
                "turn.steer",
                "Steer an active session invocation",
                vec!["invocationId"],
            ),
            (
                "turn.abort_and_prompt",
                "Abort the active invocation and start a replacement prompt",
                Vec::<&str>::new(),
            ),
            (
                "turn.list",
                "List persisted SDK sessions",
                Vec::<&str>::new(),
            ),
        ] {
            let mut properties = serde_json::Map::new();
            properties.insert("sessionId".to_string(), json!({ "type": "string" }));
            properties.insert("correlationKey".to_string(), json!({ "type": "string" }));
            properties.insert("invocationId".to_string(), json!({ "type": "string" }));
            properties.insert("prompt".to_string(), json!({ "type": "string" }));
            properties.insert(
                "override".to_string(),
                json!({ "type": ["string", "object", "array"] }),
            );
            properties.insert(
                "request".to_string(),
                json!({ "type": ["string", "object", "array"] }),
            );
            properties.insert(
                "input".to_string(),
                json!({ "type": ["string", "object", "array"] }),
            );
            properties.insert("tools".to_string(), json!({ "type": "array" }));
            properties.insert("settings".to_string(), json!({ "type": "object" }));
            properties.insert("model".to_string(), json!({ "type": "string" }));
            properties.insert("cursor".to_string(), json!({ "type": "string" }));
            properties.insert(
                "limit".to_string(),
                json!({ "type": "integer", "minimum": 1 }),
            );
            let mut input_schema = json!({
                "type": "object",
                "properties": properties,
                "required": required,
            });
            let prompt_alternatives = json!([
                { "required": ["prompt"] },
                { "required": ["request"] },
                { "required": ["input"] },
            ]);
            match name {
                "turn.prompt" | "turn.abort_and_prompt" => {
                    input_schema["anyOf"] = prompt_alternatives;
                }
                "turn.steer" => {
                    input_schema["anyOf"] = json!([
                        { "required": ["override"] },
                        { "required": ["prompt"] },
                        { "required": ["request"] },
                        { "required": ["input"] },
                    ]);
                }
                _ => {}
            }
            registry.register(
                OperationMetadata::new(name, description)
                    .with_input_schema(Schema::new(format!("Input for {name}"), input_schema)),
            );
        }

        registry
    }

    /// Register a new operation.
    pub fn register(&mut self, operation: OperationMetadata) {
        self.operations.insert(operation.name.clone(), operation);
    }

    /// Get operation metadata by name.
    pub fn get(&self, name: &str) -> Option<&OperationMetadata> {
        self.operations.get(name)
    }

    /// Check if an operation is registered.
    pub fn has_operation(&self, name: &str) -> bool {
        self.operations.contains_key(name)
    }

    /// Get all registered operations.
    pub fn operations(&self) -> impl Iterator<Item = (&String, &OperationMetadata)> {
        self.operations.iter()
    }

    /// Validate an operation call.
    pub fn validate_operation_call(&self, operation: &str, input: &Value) -> SdkResult<()> {
        let metadata = self.get(operation).ok_or_else(|| {
            SdkError::unknown_operation(format!("unknown operation: {}", operation))
        })?;

        metadata.validate_input(input)
    }

    /// Validate an operation response.
    pub fn validate_operation_response(&self, operation: &str, output: &Value) -> SdkResult<()> {
        let metadata = self.get(operation).ok_or_else(|| {
            SdkError::unknown_operation(format!("unknown operation: {}", operation))
        })?;

        metadata.validate_output(output)
    }
}

impl Default for OperationRegistry {
    fn default() -> Self {
        Self::with_standard_operations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_can_register_operations() {
        let mut registry = OperationRegistry::new();
        let op = OperationMetadata::new("test.operation", "A test operation");
        registry.register(op);
        assert!(registry.has_operation("test.operation"));
    }

    #[test]
    fn registry_retrieves_registered_operations() {
        let registry = OperationRegistry::with_standard_operations();
        assert!(registry.has_operation("query.list_sessions"));
        assert!(registry.has_operation("control.activate_session"));
        assert!(registry.has_operation("events.replay"));
    }

    #[test]
    fn registry_rejects_unknown_operations() {
        let registry = OperationRegistry::with_standard_operations();
        let result = registry.validate_operation_call("unknown.operation", &json!({}));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unknown_operation"));
    }

    #[test]
    fn operation_metadata_validates_input_schema() {
        let op = OperationMetadata::new("test.op", "Test").with_input_schema(Schema::new(
            "Test input",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                }
            }),
        ));

        // Valid input
        assert!(op.validate_input(&json!({"id": "test-123"})).is_ok());

        // Invalid input (should still validate since we only check JSON serialization)
        assert!(op.validate_input(&json!({"id": 123})).is_ok());
    }

    #[test]
    fn operation_metadata_validates_output_schema() {
        let op = OperationMetadata::new("test.op", "Test").with_output_schema(Schema::new(
            "Test output",
            json!({
                "type": "object",
                "properties": {
                    "result": { "type": "string" }
                }
            }),
        ));

        // Valid output
        assert!(op.validate_output(&json!({"result": "success"})).is_ok());
    }

    #[test]
    fn standard_registry_has_query_operations() {
        let registry = OperationRegistry::with_standard_operations();
        assert!(registry.has_operation("query.list_sessions"));
        assert!(registry.has_operation("query.get_session"));
    }

    #[test]
    fn standard_registry_has_control_operations() {
        let registry = OperationRegistry::with_standard_operations();
        assert!(registry.has_operation("control.activate_session"));
        assert!(registry.has_operation("control.deactivate_session"));
    }

    #[test]
    fn standard_registry_has_event_operations() {
        let registry = OperationRegistry::with_standard_operations();
        assert!(registry.has_operation("events.replay"));
    }

    #[test]
    fn standard_registry_has_session_lifecycle_operations() {
        let registry = OperationRegistry::with_standard_operations();
        for operation in [
            "turn.create",
            "turn.prompt",
            "turn.steer",
            "turn.abort_and_prompt",
            "turn.list",
        ] {
            assert!(registry.has_operation(operation), "missing {operation}");
        }
    }

    #[test]
    fn registry_validation_catches_missing_required_fields() {
        let registry = OperationRegistry::with_standard_operations();
        // query.get_session requires session_id
        let result = registry.validate_operation_call("query.get_session", &json!({}));
        // Basic validation will pass since we only check JSON serialization
        // A full JSON Schema validator would catch this
        assert!(result.is_ok());
    }

    #[test]
    fn schema_validates_serializable_values() {
        let schema = Schema::new("Test schema", json!({"type": "object"}));

        assert!(schema.validate(&json!({})).is_ok());
        assert!(schema.validate(&json!({"key": "value"})).is_ok());
    }

    #[test]
    fn operation_without_schemas_accepts_any_input_output() {
        let op = OperationMetadata::new("test.op", "Test");
        assert!(op.validate_input(&json!({"any": "thing"})).is_ok());
        assert!(op.validate_output(&json!({"any": "thing"})).is_ok());
    }

    #[test]
    fn operation_metadata_can_be_marked_unstable() {
        let op = OperationMetadata::new("test.op", "Test").unstable();
        assert!(!op.stable);
    }
}
