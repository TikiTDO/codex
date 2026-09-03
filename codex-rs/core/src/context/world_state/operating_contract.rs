use super::PreviousSectionState;
use super::WorldStateHash;
use super::WorldStateSection;
use crate::context::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;
use codex_protocol::protocol::InternalSessionSource;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use serde::Deserialize;
use serde::Serialize;

const OPERATING_CONTRACT: &str = include_str!("../../../assets/operating_contract.md");
const GUARDIAN_SOURCE_LABEL: &str = "guardian";

#[derive(Clone, Debug)]
pub(crate) struct OperatingContractState {
    preceding_instruction_epoch: WorldStateHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperatingContractSnapshot {
    contract: WorldStateHash,
    preceding_instruction_epoch: WorldStateHash,
}

impl OperatingContractState {
    pub(crate) fn new(preceding_instruction_epoch: WorldStateHash) -> Self {
        Self {
            preceding_instruction_epoch,
        }
    }

    pub(crate) fn applies_to(session_source: &SessionSource) -> bool {
        !matches!(
            session_source,
            SessionSource::Internal(
                InternalSessionSource::Guardian | InternalSessionSource::MemoryConsolidation
            ) | SessionSource::SubAgent(
                SubAgentSource::Review
                    | SubAgentSource::Compact
                    | SubAgentSource::MemoryConsolidation
            )
        ) && !matches!(
            session_source,
            SessionSource::SubAgent(SubAgentSource::Other(label))
                if label == GUARDIAN_SOURCE_LABEL
        )
    }
}

impl ContextualUserFragment for OperatingContractState {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("operating_contract.instructions".to_string())
    }

    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<operating_contract>", "</operating_contract>")
    }

    fn body(&self) -> String {
        format!("\n{}\n", OPERATING_CONTRACT.trim())
    }
}

impl WorldStateSection for OperatingContractState {
    const ID: &'static str = "operating_contract";
    type Snapshot = OperatingContractSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        OperatingContractSnapshot {
            contract: WorldStateHash::from_fragment(self),
            preceding_instruction_epoch: self.preceding_instruction_epoch.clone(),
        }
    }

    fn matches_legacy_fragment(role: &str, text: &str) -> bool {
        role == "developer" && Self::matches_text(text)
    }

    fn has_retained_fragment_matcher() -> bool {
        true
    }

    fn matches_retained_fragment(role: &str, text: &str) -> bool {
        Self::matches_legacy_fragment(role, text)
    }

    fn render_diff(
        &self,
        previous: PreviousSectionState<'_, Self::Snapshot>,
    ) -> Option<Box<dyn ContextualUserFragment>> {
        (!matches!(previous, PreviousSectionState::Known(previous) if previous == &self.snapshot()))
            .then(|| Box::new(self.clone()) as Box<dyn ContextualUserFragment>)
    }
}

#[cfg(test)]
#[path = "operating_contract_tests.rs"]
mod tests;
