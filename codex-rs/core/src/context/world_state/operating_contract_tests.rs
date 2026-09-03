use super::*;
use crate::context::ContextualUserFragment;
use crate::context::world_state::CollaborationModeState;
use crate::context::world_state::WorldState;
use codex_extension_api::PreviousWorldStateSection;
use codex_extension_api::RenderedWorldStateFragment;
use codex_extension_api::WorldStateSectionContribution;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use pretty_assertions::assert_eq;
use serde_json::json;

fn world_state_with_extension(value: serde_json::Value) -> WorldState {
    let expected = value.clone();
    let rendered = value.to_string();
    let mut world_state = WorldState::default();
    world_state.add_extension_section(WorldStateSectionContribution::new(
        "operating_contract_test_extension",
        value,
        move |previous| {
            if matches!(previous, PreviousWorldStateSection::Known(previous) if previous == &expected)
            {
                return None;
            }
            Some(RenderedWorldStateFragment::new(
                "developer",
                ("<test_extension>", "</test_extension>"),
                rendered.clone(),
            ))
        },
    ));
    let preceding_instruction_epoch = world_state.snapshot().stable_hash();
    world_state.add_section(OperatingContractState::new(preceding_instruction_epoch));
    world_state
}

fn world_state_with_collaboration_instructions(instructions: &str) -> WorldState {
    let mode = CollaborationMode {
        mode: ModeKind::Default,
        settings: Settings {
            model: "test-model".to_string(),
            reasoning_effort: None,
            developer_instructions: Some(instructions.to_string()),
        },
    };
    let mut world_state = WorldState::default();
    world_state.add_section(CollaborationModeState::from_collaboration_mode(
        &mode, /*catalog_messages*/ None, /*update_plan_enabled*/ true,
        /*custom_model_catalog*/ false,
    ));
    let preceding_instruction_epoch = world_state.snapshot().stable_hash();
    world_state.add_section(OperatingContractState::new(preceding_instruction_epoch));
    world_state
}

#[test]
fn changing_a_preceding_extension_reemits_the_contract_after_it() {
    let previous = world_state_with_extension(json!({"policy": "before"}));
    let current = world_state_with_extension(json!({"policy": "after"}));

    let rendered = current
        .render_diff(&previous.snapshot())
        .into_iter()
        .map(|fragment| fragment.render())
        .collect::<Vec<_>>();

    assert_eq!(rendered.len(), 2);
    assert!(rendered[0].starts_with("<test_extension>"));
    assert!(rendered[1].starts_with("<operating_contract>"));
}

#[test]
fn changing_same_mode_collaboration_text_reemits_the_contract_after_it() {
    let previous = world_state_with_collaboration_instructions("before");
    let current = world_state_with_collaboration_instructions("after");

    let rendered = current
        .render_diff(&previous.snapshot())
        .into_iter()
        .map(|fragment| fragment.render())
        .collect::<Vec<_>>();

    assert_eq!(rendered.len(), 2);
    assert!(rendered[0].starts_with("<collaboration_mode>"));
    assert!(rendered[1].starts_with("<operating_contract>"));
}

#[test]
fn unchanged_preceding_state_does_not_repeat_the_contract() {
    let previous = world_state_with_extension(json!({"policy": "same"}));
    let current = world_state_with_extension(json!({"policy": "same"}));

    assert!(current.render_diff(&previous.snapshot()).is_empty());
}

#[test]
fn snapshot_hash_is_independent_of_nested_object_insertion_order() {
    let left = world_state_with_extension(json!({"outer": {"a": 1, "b": 2}}));
    let right = world_state_with_extension(json!({"outer": {"b": 2, "a": 1}}));

    assert_eq!(
        left.snapshot().stable_hash(),
        right.snapshot().stable_hash()
    );
}

#[test]
fn contract_states_the_authority_and_ambiguity_boundaries() {
    let contract = OperatingContractState::new(
        world_state_with_extension(json!({}))
            .snapshot()
            .stable_hash(),
    )
    .render();

    assert!(contract.contains("Binding system and developer requirements remain authoritative"));
    assert!(contract.contains("Repository instructions govern work in that repository"));
    assert!(contract.contains("ambiguity can materially affect behavior"));
    assert!(contract.contains("choose a small reversible default"));
}

#[test]
fn contract_preserves_only_currently_active_persisted_goals_as_standing_work() {
    let contract = OperatingContractState::new(
        world_state_with_extension(json!({}))
            .snapshot()
            .stable_hash(),
    )
    .render();

    assert!(contract.contains("An explicitly persisted goal supplies standing work"));
    assert!(contract.contains("only while its current goal state is active"));
    assert!(contract.contains("cleared or otherwise inactive goal history does not"));
    assert!(contract.contains("Retained history alone is evidence, not a standing task"));
    assert!(!contract.contains("current user request supplies the active goal"));
}

#[test]
fn specialized_sessions_are_excluded_and_ordinary_sessions_are_included() {
    let excluded = [
        SessionSource::Internal(InternalSessionSource::Guardian),
        SessionSource::Internal(InternalSessionSource::MemoryConsolidation),
        SessionSource::SubAgent(SubAgentSource::Other(GUARDIAN_SOURCE_LABEL.to_string())),
        SessionSource::SubAgent(SubAgentSource::Review),
        SessionSource::SubAgent(SubAgentSource::Compact),
        SessionSource::SubAgent(SubAgentSource::MemoryConsolidation),
    ];
    for source in excluded {
        assert!(!OperatingContractState::applies_to(&source), "{source}");
    }

    let included = [
        SessionSource::Cli,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: Some(AgentPath::root()),
            agent_nickname: None,
            agent_role: None,
        }),
    ];
    for source in included {
        assert!(OperatingContractState::applies_to(&source), "{source}");
    }
}
