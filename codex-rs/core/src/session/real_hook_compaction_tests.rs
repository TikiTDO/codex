//! Real command publication through the actual turn loop, gated by an ordinary dynamic tool.
use super::*;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use std::time::Duration;

const HOOK_TEXT: &str = "REAL_PUBLISHED_COMPACTION_HOOK_CONTEXT";
const GATE_CALL: &str = "hook-publication-gate-call";

#[test_case::test_case(false, true, true, false; "remote summary")]
#[test_case::test_case(true, false, true, false; "token budget local summary")]
#[test_case::test_case(true, true, true, false; "token budget remote summary")]
#[test_case::test_case(true, false, false, false; "native reset")]
#[test_case::test_case(true, false, false, true; "native retry only reset")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_published_hook_reaches_actual_compaction_mechanism_once(
    token_budget: bool,
    remote: bool,
    controls: bool,
    retry: bool,
) {
    run_real_hook_turn_case(token_budget, remote, controls, retry, false).await;
}

#[test_case::test_case(false; "local summary does not wait")]
#[test_case::test_case(true; "native reset does not wait")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_hook_does_not_block_compaction_and_reaches_next_turn_once(token_budget: bool) {
    run_real_hook_turn_case(token_budget, false, !token_budget, false, true).await;
}

async fn run_real_hook_turn_case(
    token_budget: bool,
    remote: bool,
    controls: bool,
    retry: bool,
    late: bool,
) {
    let server = responses::start_mock_server().await;
    let root = tempfile::tempdir().unwrap();
    let released = root.path().join("release-hook");
    let started = root.path().join("hook-started");
    let hook_command = format!(
        "cat >/dev/null; printf started > '{}'; {}printf '%s\\n' '{}'",
        started.display(),
        if late {
            format!(
                "while [ ! -f '{}' ]; do sleep 0.01; done; ",
                released.display()
            )
        } else {
            String::new()
        },
        json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit", "additionalContext":HOOK_TEXT}}),
    );
    let request_input = json!({"instructions": controls.then_some("preserve exact ready hook"), "discard_images": controls, "retry_websocket": retry});
    let first = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_function_call(
                "compact-call",
                "compact_context",
                &request_input.to_string(),
            ),
            responses::ev_function_call(GATE_CALL, "publication_gate", "{}"),
            responses::ev_completed_with_tokens("first", 100),
        ]),
    )
    .await;
    let summary = if controls || !token_budget {
        Some(responses::mount_sse_once(&server, responses::sse(if remote {
            vec![json!({"type":"response.output_item.done", "item":{"type":"compaction", "encrypted_content":"ready-hook-summary"}}), responses::ev_completed("summary")]
        } else {
            vec![responses::ev_assistant_message("summary", "COMPACTION_SUMMARY_WITHOUT_LITERAL_HOOK"), responses::ev_completed("summary")]
        })).await)
    } else {
        None
    };
    let continuation = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_assistant_message("continued", "done"),
            responses::ev_completed("continued"),
        ]),
    )
    .await;
    let next_turn = if late {
        Some(
            responses::mount_sse_once(
                &server,
                responses::sse(vec![
                    responses::ev_assistant_message("next", "next turn done"),
                    responses::ev_completed("next"),
                ]),
            )
            .await,
        )
    } else {
        None
    };
    let mut provider = built_in_model_providers(None)["openai"].clone();
    provider.base_url = Some(format!("{}/v1", server.uri()));
    provider.supports_websockets = false;
    if !remote {
        provider.name = "OpenAI (test)".into();
    }
    let gate_tool =
        DynamicToolSpec::Function(codex_protocol::dynamic_tools::DynamicToolFunctionSpec {
            name: "publication_gate".into(),
            description: "Bounded fixture gate".into(),
            input_schema: json!({"type":"object", "properties":{}}),
            defer_loading: false,
        });
    std::fs::write(root.path().join("config.toml"), "").unwrap();
    std::fs::write(root.path().join("hooks.json"), json!({
        "hooks": {"UserPromptSubmit":[{"hooks":[{"type":"command", "command":hook_command, "async":true, "timeout":10}]}]}
    }).to_string()).unwrap();
    let (mut session, _, events) = make_session_and_context_with_auth_config_home_and_rx(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        vec![gate_tool],
        root.path(),
        move |config| {
            config.model_provider = provider;
            config.features.enable(Feature::CodexHooks).unwrap();
            config.model_context_window = Some(100_000);
            config.model_auto_compact_token_limit = Some(90_000);
            if token_budget {
                config.features.enable(Feature::TokenBudget).unwrap();
            } else {
                config.features.disable(Feature::TokenBudget).unwrap();
            }
        },
    )
    .await;
    let config = session.get_config().await;
    let (hooks, receiver) = codex_hooks::Hooks::new(
        codex_hooks::HooksConfig {
            feature_enabled: true,
            bypass_hook_trust: true,
            shell_program: Some("/bin/sh".into()),
            shell_args: vec!["-c".into()],
            config_layer_stack: Some(config.config_layer_stack.clone()),
            ..Default::default()
        },
        session.thread_id,
        Arc::new(crate::hook_mcp_executor::CoreHookMcpExecutor {
            runtime: Arc::clone(&session.services.mcp_runtime),
            thread_id: session.thread_id,
        }),
    )
    .unwrap();
    Arc::get_mut(&mut session).unwrap().async_hook_results = receiver;
    session.services.hooks.store(Arc::new(hooks));
    super::super::turn_input::handle(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "exercise compaction publication".into(),
            text_elements: Vec::new(),
        }]),
        TurnInputMode::StartOrSteer,
        "publication-turn".into(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await.unwrap().msg {
                EventMsg::DynamicToolCallRequest(call) if call.call_id == GATE_CALL => break,
                EventMsg::Error(error) => panic!("turn failed before gate: {}", error.message),
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        if late {
            while !started.exists() {
                tokio::task::yield_now().await;
            }
            assert!(session.async_hook_results.is_empty());
        } else {
            while session.async_hook_results.is_empty() {
                tokio::task::yield_now().await;
            }
            assert_eq!(session.async_hook_results.len(), 1);
        }
    })
    .await
    .expect("actual publication or deliberately unfinished process witness");
    session
        .notify_dynamic_tool_response(
            GATE_CALL,
            codex_protocol::dynamic_tools::DynamicToolResponse {
                content_items: vec![
                    codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem::InputText {
                        text: "tool gate released".into(),
                    },
                ],
                success: true,
            },
        )
        .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await.unwrap().msg {
                EventMsg::TurnComplete(_) => break,
                EventMsg::Error(error) => panic!("compaction turn failed: {}", error.message),
                _ => {}
            }
        }
    })
    .await
    .expect("compaction must complete without waiting for unfinished hook");
    assert_eq!(
        first
            .single_request()
            .body_json()
            .to_string()
            .matches(HOOK_TEXT)
            .count(),
        0
    );
    if let Some(summary) = summary {
        assert_eq!(
            summary
                .single_request()
                .body_json()
                .to_string()
                .matches(HOOK_TEXT)
                .count(),
            usize::from(!late)
        );
        assert_eq!(
            continuation
                .single_request()
                .body_json()
                .to_string()
                .matches(HOOK_TEXT)
                .count(),
            0
        );
    } else {
        assert_eq!(
            continuation
                .single_request()
                .body_json()
                .to_string()
                .matches(HOOK_TEXT)
                .count(),
            usize::from(!late)
        );
    }
    if late {
        // Existing reconfiguration preserves this in-flight command but disables the
        // next prompt's hook: exactly one source result must reach the next turn.
        let hooks = session
            .services
            .hooks
            .load_full()
            .reconfigured(Default::default());
        session.services.hooks.store(Arc::new(hooks));
        std::fs::write(&released, "release").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while session.async_hook_results.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        super::super::turn_input::handle(
            &session,
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: "next ordinary turn".into(),
                text_elements: Vec::new(),
            }]),
            TurnInputMode::StartOrSteer,
            "after-late-publication".into(),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !matches!(events.recv().await.unwrap().msg, EventMsg::TurnComplete(_)) {}
        })
        .await
        .unwrap();
        assert_eq!(
            next_turn
                .unwrap()
                .single_request()
                .body_json()
                .to_string()
                .matches(HOOK_TEXT)
                .count(),
            1
        );
    }
    assert!(session.async_hook_results.is_empty());
    session.services.hooks.load_full().shutdown().await;
}
