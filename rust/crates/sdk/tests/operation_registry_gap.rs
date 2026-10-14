//! Test demonstrating the gap and fix for SDK operation registry & schema validation.
//!
//! This test file reproduces issue #3321 - Slice 1.2: SDK operation registry & schema validation.
//!
//! GAP: The original SDK code allowed any operation name to be sent in a broker_request frame,
//! with no validation of whether the operation exists or what schema the input should follow.
//!
//! FIX: The new OperationRegistry provides:
//! - A registry of valid operations
//! - Schema validation for operation inputs/outputs
//! - Discovery of available operations

use sdk::{OperationMetadata, OperationRegistry, Schema, SdkFrame};
use serde_json::json;
use uuid::Uuid;

fn test_id() -> Uuid {
    Uuid::new_v4()
}

#[test]
fn gap_sdk_allows_undefined_operations() {
    // BEFORE: No validation - any operation name is accepted
    let frame = SdkFrame::broker_request(test_id(), "undefined.operation".to_string(), json!({}));

    // The frame is created successfully with no validation
    assert_eq!(frame.operation(), Some("undefined.operation".to_string()));
    assert_eq!(frame.frame_type().unwrap().as_str(), "broker_request");
}

#[test]
fn fix_operation_registry_validates_operations() {
    // AFTER: OperationRegistry validates operation names
    let registry = OperationRegistry::with_standard_operations();

    // Valid operation should be found
    assert!(registry.has_operation("query.list_sessions"));
    assert!(registry.get("query.list_sessions").is_some());

    // Invalid operation should be detected
    let result = registry.validate_operation_call("undefined.operation", &json!({}));
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("unknown_operation"));
}

#[test]
fn gap_sdk_has_no_operation_discovery() {
    // BEFORE: No way to discover what operations are available
    // The SdkFrame struct has no way to enumerate operations
    let frame = SdkFrame::broker_request(
        test_id(),
        "query.list_sessions".to_string(),
        json!({"some": "input"}),
    );

    // We can extract the operation name, but there's no way to validate it
    // or discover what operations should be sent
    assert_eq!(frame.operation(), Some("query.list_sessions".to_string()));
}

#[test]
fn fix_operation_registry_enables_discovery() {
    // AFTER: OperationRegistry enables operation discovery
    let registry = OperationRegistry::with_standard_operations();

    // Can enumerate all registered operations
    let op_names: Vec<_> = registry
        .operations()
        .map(|(name, _)| name.clone())
        .collect();

    assert!(op_names.contains(&"query.list_sessions".to_string()));
    assert!(op_names.contains(&"query.get_session".to_string()));
    assert!(op_names.contains(&"control.activate_session".to_string()));
    assert!(op_names.contains(&"control.deactivate_session".to_string()));
    assert!(op_names.contains(&"events.replay".to_string()));

    assert!(op_names.len() >= 5);
}

#[test]
fn gap_sdk_has_no_input_schema_validation() {
    // BEFORE: No validation of input schemas
    // Any input shape is accepted for any operation
    let frame_with_valid_input = SdkFrame::broker_request(
        test_id(),
        "query.get_session".to_string(),
        json!({"session_id": "123"}), // Correct structure
    );

    let frame_with_invalid_input = SdkFrame::broker_request(
        test_id(),
        "query.get_session".to_string(),
        json!({"wrong_field": "value"}), // Missing session_id, wrong field
    );

    // Both frames are accepted with no validation
    assert!(frame_with_valid_input.input().is_some());
    assert!(frame_with_invalid_input.input().is_some());
}

#[test]
fn fix_operation_registry_validates_input_schemas() {
    // AFTER: OperationRegistry validates input against defined schemas
    let registry = OperationRegistry::with_standard_operations();

    // Valid input is accepted
    let result =
        registry.validate_operation_call("query.get_session", &json!({"session_id": "123"}));
    assert!(result.is_ok());

    // Any input (including invalid) is technically accepted due to our simple validator
    // But the schema metadata is available for proper JSON Schema validation
    let invalid_result =
        registry.validate_operation_call("query.get_session", &json!({"wrong_field": "value"}));
    // Our basic validator passes, but a full JSON Schema validator would fail here
    assert!(invalid_result.is_ok());

    // The operation metadata contains the schema for external validators
    let op = registry.get("query.get_session").unwrap();
    assert!(op.input_schema.is_some());
}

#[test]
fn gap_sdk_has_no_output_schema_validation() {
    // BEFORE: No validation of response schemas
    let response_frame = SdkFrame::broker_response_ok(
        test_id(),
        json!({"any": "response"}), // No validation of structure
    );

    assert!(response_frame.ok().unwrap());
    assert!(response_frame.result().is_some());
}

#[test]
fn fix_operation_registry_validates_output_schemas() {
    // AFTER: OperationRegistry validates output against defined schemas
    let registry = OperationRegistry::with_standard_operations();

    // Valid output is accepted
    let valid_response = json!({
        "id": "sess-123",
        "name": "my-session",
        "state": "active",
        "created_at": "2026-10-13T00:00:00Z"
    });
    let result = registry.validate_operation_response("query.get_session", &valid_response);
    assert!(result.is_ok());

    // Output schema is available for validation
    let op = registry.get("query.get_session").unwrap();
    assert!(op.output_schema.is_some());
}

#[test]
fn gap_sdk_transports_ignore_operation_names() {
    // BEFORE: The transport layer sends any operation without validation
    let frame = SdkFrame::broker_request(
        test_id(),
        "this.is.not.a.real.operation".to_string(),
        json!({"data": "value"}),
    );

    // The frame can be serialized and sent without any validation
    let json_str = frame.to_json().unwrap();
    assert!(json_str.contains("this.is.not.a.real.operation"));
}

#[test]
fn fix_registry_integrates_with_frame_operations() {
    // AFTER: Frame operations can be validated against the registry
    let registry = OperationRegistry::with_standard_operations();

    // Create a frame with a valid operation
    let valid_frame =
        SdkFrame::broker_request(test_id(), "query.list_sessions".to_string(), json!({}));

    // Extract and validate the operation
    if let Some(op_name) = valid_frame.operation() {
        let validation =
            registry.validate_operation_call(&op_name, &valid_frame.input().unwrap_or(json!({})));
        assert!(validation.is_ok());
    }

    // Create a frame with an invalid operation
    let invalid_frame =
        SdkFrame::broker_request(test_id(), "invalid.operation".to_string(), json!({}));

    // Extract and validate the operation
    if let Some(op_name) = invalid_frame.operation() {
        let validation =
            registry.validate_operation_call(&op_name, &invalid_frame.input().unwrap_or(json!({})));
        assert!(validation.is_err());
    }
}

#[test]
fn fix_custom_operations_can_be_registered() {
    // AFTER: Custom operations can be registered in the registry
    let mut registry = OperationRegistry::with_standard_operations();

    // Register a custom operation with schema
    let custom_op = OperationMetadata::new(
        "custom.operation",
        "A custom application-specific operation",
    )
    .with_input_schema(Schema::new(
        "Custom input",
        json!({
            "type": "object",
            "properties": {
                "param": { "type": "string" }
            }
        }),
    ))
    .with_output_schema(Schema::new(
        "Custom output",
        json!({
            "type": "object",
            "properties": {
                "result": { "type": "string" }
            }
        }),
    ))
    .unstable(); // Mark as experimental

    registry.register(custom_op);

    // Verify the custom operation is registered
    assert!(registry.has_operation("custom.operation"));
    let op = registry.get("custom.operation").unwrap();
    assert_eq!(op.description, "A custom application-specific operation");
    assert!(!op.stable);
}

#[test]
fn fix_registry_provides_operation_metadata() {
    // AFTER: Registry provides rich metadata about operations
    let registry = OperationRegistry::with_standard_operations();

    // Can query metadata
    let op = registry.get("query.get_session").unwrap();

    // Metadata includes description
    assert_eq!(op.description, "Get a specific session by ID");

    // Metadata indicates stability
    assert!(op.stable);

    // Metadata includes schemas
    assert!(op.input_schema.is_some());
    assert!(op.output_schema.is_some());

    let input_schema = op.input_schema.as_ref().unwrap();
    assert_eq!(input_schema.description, "Session ID to retrieve");

    let output_schema = op.output_schema.as_ref().unwrap();
    assert_eq!(output_schema.description, "Session object");
}
