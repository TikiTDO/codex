//! Measure the reviewed design specimen through existing native descriptor consumers.
//! This is not an estate backend, installed profile, provider call or effect test.

#![allow(clippy::expect_used)]

use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_tools::augment_tool_spec_for_code_mode;
use codex_tools::mcp_tool_to_responses_api_tool;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;

// Independent policy oracle: these keywords are absent from native JsonSchema.
// Do not derive expected output by calling the converter under inspection.
fn supported_subset(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "minLength" | "pattern" | "minimum" | "maximum" | "maxItems"
                    )
                })
                .map(|(key, value)| (key.clone(), supported_subset(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(supported_subset).collect()),
        other => other.clone(),
    }
}

#[test]
fn estate_design_default_policy_preserves_closed_direct_and_code_mode_descriptors() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/fixtures/estate-tool-schema-design.json"
    ))
    .expect("complete design fixture");
    let tools = fixture["tools"].as_array().expect("design tools");
    assert_eq!(tools.len(), 6);
    let expected_operations = [
        vec!["inspect"],
        vec!["recent", "show", "question", "effect_status"],
        vec![
            "wake", "catchup", "publish", "comment", "question", "answer", "answer", "close",
            "release", "cancel",
        ],
        vec!["now", "since", "channels", "cursor_status"],
        vec!["catchup", "adopt"],
        vec!["literal", "semantic", "show"],
    ];
    let mut measurements = Vec::new();
    for (tool, operations) in tools.iter().zip(expected_operations) {
        let name = tool["name"].as_str().expect("tool name");
        let wire = &tool["inputSchema"];
        let wire_bytes = serde_json::to_vec(wire).expect("wire schema bytes").len();
        assert!(wire_bytes <= 5_000, "{name}: unmeasured policy increase");
        let branches = wire["oneOf"].as_array().expect("closed operation branches");
        assert_eq!(branches.len(), operations.len(), "{name}");
        for (branch, operation) in branches.iter().zip(&operations) {
            assert_eq!(branch["type"], "object", "{name}/{operation}");
            assert_eq!(branch["additionalProperties"], false, "{name}/{operation}");
            assert_eq!(
                branch["properties"]["operation"]["enum"],
                json!([operation]),
                "{name}/{operation}"
            );
        }
        let mcp = rmcp::model::Tool::new(
            name.to_string(),
            tool["description"]
                .as_str()
                .expect("description")
                .to_string(),
            Arc::new(wire.as_object().expect("input schema object").clone()),
        );
        let direct = mcp_tool_to_responses_api_tool(
            &ToolName::plain(name.to_string()),
            &mcp,
            // Exercise the ordinary existing default, not an estate override.
            None,
        )
        .expect("direct consumer");
        let direct_schema = serde_json::to_value(&direct.parameters).expect("direct schema");
        let mut expected = supported_subset(wire);
        // The existing MCP adapter inserts this root field, not a permissive branch.
        expected["properties"] = json!({});
        assert_eq!(direct_schema, expected, "{name}: full supported contract");
        assert!(
            !direct.strict,
            "{name}: provider grammar is not enforcement"
        );
        let direct_bytes = serde_json::to_vec(&direct)
            .expect("direct descriptor")
            .len();
        let code_mode = augment_tool_spec_for_code_mode(ToolSpec::Function(direct), None);
        let ToolSpec::Function(code_mode) = code_mode else {
            panic!("function descriptor changed kind");
        };
        assert_eq!(
            serde_json::to_value(&code_mode.parameters).expect("code-mode schema"),
            expected,
            "{name}: code-mode parameters widened"
        );
        assert!(
            !code_mode.description.contains("args: unknown"),
            "{name}: renderer exhausted its ordinary budget"
        );
        for operation in operations {
            assert!(
                code_mode
                    .description
                    .contains(&format!("operation: \"{operation}\"")),
                "{name}/{operation}: missing code-mode discriminator"
            );
        }
        if name == "desk_act_v2" {
            assert!(
                code_mode
                    .description
                    .contains("2..6 stable key/label pairs")
            );
            assert!(code_mode.description.contains("canonical lowercase UUIDs"));
        }
        measurements.push(json!({
            "tool": name,
            "wire_schema_utf8_bytes": wire_bytes,
            "direct_schema_utf8_bytes": serde_json::to_vec(&direct_schema).expect("schema bytes").len(),
            "direct_descriptor_utf8_bytes": direct_bytes,
            "code_mode_descriptor_utf8_bytes": serde_json::to_vec(&code_mode).expect("code-mode bytes").len(),
        }));
    }
    println!(
        "ESTATE_DESIGN_DESCRIPTOR_BYTES {}",
        serde_json::to_string(&measurements).expect("measurement receipt")
    );
}
