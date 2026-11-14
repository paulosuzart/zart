//! Minimal durable execution harness for [AutoAgents](https://github.com/liquidos-ai/AutoAgents)
//! backed by [Zart](https://crates.io/crates/zart).
//!
//! # Architecture
//!
//! Instead of wrapping an entire `ReActAgent` in a single `#[zart_step]` (which would
//! lose intra-loop progress on crash), this crate wraps the two side-effect boundaries
//! inside the agent loop:
//!
//! - [`DurableLLMProvider`] wraps `Arc<dyn LLMProvider>` and persists each LLM turn as a
//!   deterministic Zart step (`llm-call-0`, `llm-call-1`, ...).
//! - [`DurableTool`] wraps `Arc<dyn ToolT>` and persists each tool execution as a
//!   deterministic Zart step (`tool-<name>-0`, `tool-<name>-1`, ...).
//! - [`ControlFlowSlot`] tunnels Zart control-flow signals (`StepError::Scheduled`,
//!   `StepError::StepExecuted`) across AutoAgents' `LLMError` / `ToolCallError`
//!   boundaries so [`run_durable_agent`] can propagate them to the Zart worker.
//! - [`wait_for_hitl`] suspends the agent mid-loop via `zart::wait_for_event` for
//!   human-in-the-loop approvals or deferred client tool calls.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use autoagents::core::agent::task::Task;
use autoagents::core::agent::{AgentDeriveT, AgentHooks, Context, HookOutcome};
use autoagents::core::tool::{ToolCallError, ToolRuntime, ToolT};
use autoagents::llm::chat::{
    ChatMessage, ChatProvider, ChatResponse, ChatRole, FunctionTool, MessageType,
    SamplingOverrides, StructuredOutputFormat, Tool, Usage,
};
use autoagents::llm::completion::{CompletionProvider, CompletionRequest, CompletionResponse};
use autoagents::llm::embedding::EmbeddingProvider;
use autoagents::llm::error::LLMError;
use autoagents::llm::models::{ModelListRequest, ModelListResponse, ModelsProvider};
use autoagents::llm::{FunctionCall, LLMProvider, ToolCall};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zart::error::{StepError, StepOutcome, TaskError};
use zart::registry::DurableExecution;
use zart::{RetryConfig, ZartStep};

// ── Control-flow tunneling ────────────────────────────────────────────────────

/// Shared slot that captures Zart control-flow signals (`StepError::Scheduled` /
/// `StepError::StepExecuted`) when they occur inside `LLMProvider` or `ToolRuntime`
/// callbacks, where the return type is `LLMError` or `ToolCallError` rather than
/// `zart::TaskError`.
#[derive(Debug, Clone, Default)]
pub struct ControlFlowSlot {
    pending: Arc<Mutex<Option<StepError>>>,
}

impl ControlFlowSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a Zart control-flow error if the slot is currently empty.
    pub fn capture(&self, err: StepError) {
        let mut guard = self.pending.lock().expect("control flow mutex poisoned");
        if guard.is_none() {
            *guard = Some(err);
        }
    }

    /// Returns `true` if a control-flow signal has already been captured.
    pub fn is_captured(&self) -> bool {
        self.pending
            .lock()
            .expect("control flow mutex poisoned")
            .is_some()
    }

    /// Take the captured `StepError`, if any.
    pub fn take(&self) -> Option<StepError> {
        self.pending
            .lock()
            .expect("control flow mutex poisoned")
            .take()
    }
}

/// Runs an AutoAgents future inside a `DurableExecution::run` body and converts
/// any captured Zart control-flow signal back into `Err(TaskError)`.
pub async fn run_durable_agent<T, E, F, Fut>(slot: &ControlFlowSlot, f: F) -> Result<T, TaskError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let res = f().await;
    if let Some(step_err) = slot.take() {
        return Err(TaskError::from(step_err));
    }
    res.map_err(|e| {
        tracing::error!(error = %e, "Durable agent execution failed");
        TaskError::StepFailed {
            step: "autoagents".to_string(),
            source: StepError::Failed {
                step: "autoagents".to_string(),
                reason: e.to_string(),
            },
        }
    })
}

// ── Serializable ChatResponse ─────────────────────────────────────────────────

/// Serializable representation of an AutoAgents [`ChatResponse`].
///
/// Zart persists step outputs as JSON in PostgreSQL. Because `ChatProvider`
/// returns `Box<dyn ChatResponse>`, [`DurableLLMProvider`] converts every
/// provider response into `DurableChatResponse` before persisting the step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DurableChatResponse {
    pub text: Option<String>,
    pub tool_calls: Option<Vec<ToolCall>>,
    pub thinking: Option<String>,
    pub usage: Option<Usage>,
}

impl DurableChatResponse {
    pub fn from_response(resp: &dyn ChatResponse) -> Self {
        Self {
            text: resp.text(),
            tool_calls: resp.tool_calls(),
            thinking: resp.thinking(),
            usage: resp.usage(),
        }
    }
}

impl fmt::Display for DurableChatResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(text) = &self.text {
            write!(f, "{text}")
        } else if let Some(calls) = &self.tool_calls {
            write!(f, "[{} tool call(s)]", calls.len())
        } else {
            Ok(())
        }
    }
}

impl ChatResponse for DurableChatResponse {
    fn text(&self) -> Option<String> {
        self.text.clone()
    }

    fn tool_calls(&self) -> Option<Vec<ToolCall>> {
        self.tool_calls.clone()
    }

    fn thinking(&self) -> Option<String> {
        self.thinking.clone()
    }

    fn usage(&self) -> Option<Usage> {
        self.usage.clone()
    }
}

// ── Step error ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("Step '{step}' failed: {reason}")]
struct BridgeStepError {
    step: String,
    reason: String,
}

// ── DurableLLMProvider ────────────────────────────────────────────────────────

/// Wraps an `Arc<dyn LLMProvider>` so every `chat_with_tools` call is persisted
/// as a deterministic Zart step (`llm-call-0`, `llm-call-1`, ...).
pub struct DurableLLMProvider {
    inner: Arc<dyn LLMProvider>,
    turn: AtomicUsize,
    slot: ControlFlowSlot,
    step_prefix: String,
    retry: Option<RetryConfig>,
}

impl DurableLLMProvider {
    pub fn new(inner: Arc<dyn LLMProvider>, slot: ControlFlowSlot) -> Self {
        Self {
            inner,
            turn: AtomicUsize::new(0),
            slot,
            step_prefix: "llm-call".to_string(),
            retry: None,
        }
    }

    pub fn with_step_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.step_prefix = prefix.into();
        self
    }

    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }
}

struct LlmChatStep<'a> {
    name: String,
    inner: Arc<dyn LLMProvider>,
    messages: &'a [ChatMessage],
    tools: Option<&'a [Tool]>,
    json_schema: Option<StructuredOutputFormat>,
    sampling: Option<&'a SamplingOverrides>,
    retry: Option<RetryConfig>,
}

impl ZartStep for LlmChatStep<'_> {
    type Output = DurableChatResponse;
    type Error = BridgeStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.name.clone())
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        self.retry.clone()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let resp = match self.sampling {
            Some(s) => {
                self.inner
                    .chat_with_tools_and_sampling(
                        self.messages,
                        self.tools,
                        self.json_schema.clone(),
                        Some(s),
                    )
                    .await
            }
            None => {
                self.inner
                    .chat_with_tools(self.messages, self.tools, self.json_schema.clone())
                    .await
            }
        }
        .map_err(|e| {
            tracing::warn!(step = %self.name, error = %e, "LLM step call failed");
            BridgeStepError {
                step: self.name.clone(),
                reason: e.to_string(),
            }
        })?;

        let durable_resp = DurableChatResponse::from_response(resp.as_ref());
        tracing::info!(
            step = %self.name,
            text_len = durable_resp.text.as_ref().map(|t| t.len()).unwrap_or(0),
            tool_calls = durable_resp.tool_calls.as_ref().map(|c| c.len()).unwrap_or(0),
            "LLM step completed"
        );
        Ok(durable_resp)
    }
}

#[async_trait]
impl ChatProvider for DurableLLMProvider {
    async fn chat_with_tools(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[Tool]>,
        json_schema: Option<StructuredOutputFormat>,
    ) -> Result<Box<dyn ChatResponse>, LLMError> {
        self.chat_with_tools_and_sampling(messages, tools, json_schema, None)
            .await
    }

    async fn chat_with_tools_and_sampling(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[Tool]>,
        json_schema: Option<StructuredOutputFormat>,
        sampling: Option<&SamplingOverrides>,
    ) -> Result<Box<dyn ChatResponse>, LLMError> {
        // Abort immediately if an earlier tool call already triggered a Zart suspension.
        if self.slot.is_captured() {
            return Err(LLMError::ProviderError(
                "zart execution suspended by control-flow signal".to_string(),
            ));
        }

        let turn = self.turn.fetch_add(1, Ordering::SeqCst);
        let step_name = format!("{}-{turn}", self.step_prefix);

        let step = LlmChatStep {
            name: step_name,
            inner: Arc::clone(&self.inner),
            messages,
            tools,
            json_schema,
            sampling,
            retry: self.retry.clone(),
        };

        match zart::step(step).await {
            Ok(StepOutcome::Ok(cached)) => Ok(Box::new(cached)),
            Ok(StepOutcome::BusinessErr(e)) => Err(LLMError::ProviderError(e.to_string())),
            Ok(StepOutcome::ZartErr(e)) => Err(LLMError::ProviderError(e.to_string())),
            Err(step_err) => {
                self.slot.capture(step_err);
                Err(LLMError::ProviderError(
                    "zart step scheduled (control flow)".to_string(),
                ))
            }
        }
    }
}

#[async_trait]
impl CompletionProvider for DurableLLMProvider {
    async fn complete(
        &self,
        req: &CompletionRequest,
        json_schema: Option<StructuredOutputFormat>,
    ) -> Result<CompletionResponse, LLMError> {
        self.inner.complete(req, json_schema).await
    }
}

#[async_trait]
impl EmbeddingProvider for DurableLLMProvider {
    async fn embed(&self, input: Vec<String>) -> Result<Vec<Vec<f32>>, LLMError> {
        self.inner.embed(input).await
    }
}

#[async_trait]
impl ModelsProvider for DurableLLMProvider {
    async fn list_models(
        &self,
        request: Option<&ModelListRequest>,
    ) -> Result<Box<dyn ModelListResponse>, LLMError> {
        self.inner.list_models(request).await
    }
}

impl LLMProvider for DurableLLMProvider {}

// ── DurableTool ───────────────────────────────────────────────────────────────

/// Wraps an `Arc<dyn ToolT>` so each invocation becomes a deterministic Zart
/// step (`tool-<name>-0`, `tool-<name>-1`, ...).
#[derive(Debug)]
pub struct DurableTool {
    inner: Arc<dyn ToolT>,
    call_count: AtomicUsize,
    slot: ControlFlowSlot,
    retry: Option<RetryConfig>,
}

impl DurableTool {
    pub fn new(inner: Arc<dyn ToolT>, slot: ControlFlowSlot) -> Self {
        Self {
            inner,
            call_count: AtomicUsize::new(0),
            slot,
            retry: None,
        }
    }

    pub fn from_box(inner: Box<dyn ToolT>, slot: ControlFlowSlot) -> Self {
        Self::new(Arc::from(inner), slot)
    }

    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Convenience helper to wrap a list of tools sharing the same [`ControlFlowSlot`].
    pub fn wrap_all(tools: Vec<Arc<dyn ToolT>>, slot: &ControlFlowSlot) -> Vec<Box<dyn ToolT>> {
        tools
            .into_iter()
            .map(|t| Box::new(Self::new(t, slot.clone())) as Box<dyn ToolT>)
            .collect()
    }
}

struct ToolExecStep {
    name: String,
    inner: Arc<dyn ToolT>,
    args: Value,
    retry: Option<RetryConfig>,
}

impl ZartStep for ToolExecStep {
    type Output = Value;
    type Error = BridgeStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.name.clone())
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        self.retry.clone()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        self.inner
            .execute(self.args.clone())
            .await
            .map_err(|e| BridgeStepError {
                step: self.name.clone(),
                reason: e.to_string(),
            })
    }
}

#[async_trait]
impl ToolRuntime for DurableTool {
    async fn execute(&self, args: Value) -> Result<Value, ToolCallError> {
        if self.slot.is_captured() {
            return Err(ToolCallError::RuntimeError(
                "zart execution suspended by control-flow signal".into(),
            ));
        }

        let idx = self.call_count.fetch_add(1, Ordering::SeqCst);
        let step_name = format!("tool-{}-{idx}", self.inner.name());

        let step = ToolExecStep {
            name: step_name,
            inner: Arc::clone(&self.inner),
            args,
            retry: self.retry.clone(),
        };

        match zart::step(step).await {
            Ok(StepOutcome::Ok(val)) => Ok(val),
            Ok(StepOutcome::BusinessErr(e)) => {
                Err(ToolCallError::RuntimeError(e.to_string().into()))
            }
            Ok(StepOutcome::ZartErr(e)) => Err(ToolCallError::RuntimeError(e.to_string().into())),
            Err(step_err) => {
                self.slot.capture(step_err);
                Err(ToolCallError::RuntimeError(
                    "zart step scheduled (control flow)".into(),
                ))
            }
        }
    }
}

impl ToolT for DurableTool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn description(&self) -> &str {
        self.inner.description()
    }

    fn args_schema(&self) -> Value {
        self.inner.args_schema()
    }

    fn output_schema(&self) -> Option<Value> {
        self.inner.output_schema()
    }
}

// ── Human-in-the-loop helper ──────────────────────────────────────────────────

/// Suspends the agent mid-loop waiting for an external Zart event
/// (`DurableScheduler::offer_event`), tunneling any `StepError::Scheduled`
/// control-flow signal through [`ControlFlowSlot`].
pub async fn wait_for_hitl<T: DeserializeOwned>(
    slot: &ControlFlowSlot,
    event_name: &str,
    timeout: Option<Duration>,
) -> Result<T, ToolCallError> {
    if slot.is_captured() {
        return Err(ToolCallError::RuntimeError(
            "zart execution suspended by control-flow signal".into(),
        ));
    }

    match zart::wait_for_event::<T>(event_name, timeout).await {
        Ok(val) => Ok(val),
        Err(step_err) => {
            slot.capture(step_err);
            Err(ToolCallError::RuntimeError(
                format!("waiting for HITL event '{event_name}'").into(),
            ))
        }
    }
}

// ── DurableAgent wrapper ──────────────────────────────────────────────────────

/// Wraps any `AgentDeriveT + AgentHooks` (such as a struct annotated with `#[agent]`)
/// so that every tool returned by `T::tools()` is automatically wrapped in [`DurableTool`].
///
/// Use with `ReActAgent::new(DurableAgent::new(my_agent, slot.clone()))`.
#[derive(Debug)]
pub struct DurableAgent<T> {
    inner: T,
    slot: ControlFlowSlot,
}

impl<T> DurableAgent<T> {
    pub fn new(inner: T, slot: ControlFlowSlot) -> Self {
        Self { inner, slot }
    }
}

#[async_trait]
impl<T: AgentDeriveT + Send + Sync> AgentDeriveT for DurableAgent<T> {
    type Output = T::Output;

    fn description(&self) -> &str {
        self.inner.description()
    }

    fn output_schema(&self) -> Option<Value> {
        self.inner.output_schema()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn tools(&self) -> Vec<Box<dyn ToolT>> {
        self.inner
            .tools()
            .into_iter()
            .map(|t| Box::new(DurableTool::from_box(t, self.slot.clone())) as Box<dyn ToolT>)
            .collect()
    }
}

#[async_trait]
impl<T: AgentHooks + Send + Sync> AgentHooks for DurableAgent<T> {
    async fn on_agent_create(&self) {
        self.inner.on_agent_create().await
    }

    async fn on_run_start(&self, task: &Task, ctx: &Context) -> HookOutcome {
        self.inner.on_run_start(task, ctx).await
    }

    async fn on_run_complete(&self, task: &Task, result: &Self::Output, ctx: &Context)
    where
        Self::Output: 'static,
    {
        self.inner.on_run_complete(task, result, ctx).await
    }
}

// ── Reusable DurableExecution for ReAct agents ────────────────────────────────

/// Input payload for [`DurableReactTask`].
///
/// Wrapped as a JSON object struct (rather than a bare `String`) so Zart's
/// `zart::schedule` / `zart::wait` WaitGroup machinery can attach internal
/// fields (`wg_step_name`) without breaking deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPrompt {
    pub prompt: String,
}

impl AgentPrompt {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
        }
    }
}

impl From<String> for AgentPrompt {
    fn from(prompt: String) -> Self {
        Self { prompt }
    }
}

impl From<&str> for AgentPrompt {
    fn from(prompt: &str) -> Self {
        Self {
            prompt: prompt.to_string(),
        }
    }
}

/// Reusable [`DurableExecution`] implementation for any AutoAgents `#[agent]`
/// (`AgentDeriveT`).
///
/// Drives the ReAct loop directly on top of Zart primitives:
/// - Each LLM turn is persisted via `zart::step` (`llm-call-0`, `llm-call-1`, ...).
/// - When a turn emits **1 tool call**, it executes via `zart::step` (`tool-{turn}-0-{name}`).
/// - When a turn emits **multiple tool calls**, they are dispatched **in parallel** via
///   `zart::schedule` + `zart::wait` (`tool-{turn}-{idx}-{name}`).
pub struct DurableReactTask<A> {
    agent: A,
    llm: Arc<dyn LLMProvider>,
    max_turns: usize,
    llm_retry: Option<RetryConfig>,
    tool_retry: Option<RetryConfig>,
}

impl<A> DurableReactTask<A> {
    pub fn new(agent: A, llm: Arc<dyn LLMProvider>) -> Self {
        Self {
            agent,
            llm,
            max_turns: 10,
            llm_retry: None,
            tool_retry: None,
        }
    }

    pub fn with_max_turns(mut self, max_turns: usize) -> Self {
        self.max_turns = max_turns.max(1);
        self
    }

    pub fn with_llm_retry(mut self, retry: RetryConfig) -> Self {
        self.llm_retry = Some(retry);
        self
    }

    pub fn with_tool_retry(mut self, retry: RetryConfig) -> Self {
        self.tool_retry = Some(retry);
        self
    }
}

struct OwnedLlmTurnStep {
    name: String,
    inner: Arc<dyn LLMProvider>,
    messages: Vec<ChatMessage>,
    tools: Vec<Tool>,
    retry: Option<RetryConfig>,
}

impl ZartStep for OwnedLlmTurnStep {
    type Output = DurableChatResponse;
    type Error = BridgeStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.name.clone())
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        self.retry.clone()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let tools_slice = if self.tools.is_empty() {
            None
        } else {
            Some(self.tools.as_slice())
        };
        let resp = self
            .inner
            .chat_with_tools(&self.messages, tools_slice, None)
            .await
            .map_err(|e| {
                tracing::warn!(step = %self.name, error = %e, "LLM step call failed");
                BridgeStepError {
                    step: self.name.clone(),
                    reason: e.to_string(),
                }
            })?;

        let durable_resp = DurableChatResponse::from_response(resp.as_ref());
        tracing::info!(
            step = %self.name,
            text_len = durable_resp.text.as_ref().map(|t| t.len()).unwrap_or(0),
            tool_calls = durable_resp.tool_calls.as_ref().map(|c| c.len()).unwrap_or(0),
            "LLM step completed"
        );
        Ok(durable_resp)
    }
}

#[async_trait]
impl<A> DurableExecution for DurableReactTask<A>
where
    A: AgentDeriveT + Send + Sync + 'static,
{
    type Data = AgentPrompt;
    type Output = String;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let prompt = data.prompt;
        let raw_tools: Vec<Arc<dyn ToolT>> =
            self.agent.tools().into_iter().map(Arc::from).collect();

        let tool_defs: Vec<Tool> = raw_tools
            .iter()
            .map(|t| Tool {
                tool_type: "function".to_string(),
                function: FunctionTool {
                    name: t.name().to_string(),
                    description: t.description().to_string(),
                    parameters: t.args_schema(),
                },
            })
            .collect();

        let tool_map: HashMap<String, Arc<dyn ToolT>> = raw_tools
            .into_iter()
            .map(|t| (t.name().to_string(), t))
            .collect();

        let mut messages = vec![
            ChatMessage {
                role: ChatRole::System,
                message_type: MessageType::Text,
                content: self.agent.description().to_string(),
            },
            ChatMessage {
                role: ChatRole::User,
                message_type: MessageType::Text,
                content: prompt,
            },
        ];

        for turn in 0..self.max_turns {
            let llm_step = OwnedLlmTurnStep {
                name: format!("llm-call-{turn}"),
                inner: Arc::clone(&self.llm),
                messages: messages.clone(),
                tools: tool_defs.clone(),
                retry: self.llm_retry.clone(),
            };

            let resp = zart::require(llm_step).await?;
            let response_text = resp.text.unwrap_or_default();
            let tool_calls = resp.tool_calls.unwrap_or_default();

            if tool_calls.is_empty() {
                return Ok(response_text);
            }

            let tool_outputs: Vec<Value> = if tool_calls.len() == 1 {
                let call = &tool_calls[0];
                let step = build_tool_step(
                    format!("tool-{turn}-0-{}", call.function.name),
                    call,
                    &tool_map,
                    self.tool_retry.clone(),
                )?;
                vec![zart::require(step).await?]
            } else {
                tracing::info!(
                    turn,
                    count = tool_calls.len(),
                    "Dispatching tool calls in parallel via zart::schedule"
                );
                let mut handles = Vec::with_capacity(tool_calls.len());
                for (idx, call) in tool_calls.iter().enumerate() {
                    let step = build_tool_step(
                        format!("tool-{turn}-{idx}-{}", call.function.name),
                        call,
                        &tool_map,
                        self.tool_retry.clone(),
                    )?;
                    handles.push(zart::schedule(step));
                }
                let results = zart::wait(handles).await?;
                let mut values = Vec::with_capacity(results.len());
                for (idx, res) in results.into_iter().enumerate() {
                    let val = res.map_err(|e| TaskError::StepFailed {
                        step: format!("tool-{turn}-{idx}-{}", tool_calls[idx].function.name),
                        source: e,
                    })?;
                    values.push(val);
                }
                values
            };

            let result_calls: Vec<ToolCall> = tool_calls
                .iter()
                .zip(tool_outputs.iter())
                .map(|(call, val)| ToolCall {
                    id: call.id.clone(),
                    call_type: call.call_type.clone(),
                    function: FunctionCall {
                        name: call.function.name.clone(),
                        arguments: serde_json::to_string(val).unwrap_or_default(),
                    },
                })
                .collect();

            messages.push(ChatMessage {
                role: ChatRole::Assistant,
                message_type: MessageType::ToolUse(tool_calls),
                content: response_text,
            });
            messages.push(ChatMessage {
                role: ChatRole::Tool,
                message_type: MessageType::ToolResult(result_calls),
                content: String::new(),
            });
        }

        Err(TaskError::StepFailed {
            step: "react-loop".to_string(),
            source: StepError::Failed {
                step: "react-loop".to_string(),
                reason: format!("max turns ({}) exceeded", self.max_turns),
            },
        })
    }
}

fn build_tool_step(
    step_name: String,
    call: &ToolCall,
    tool_map: &HashMap<String, Arc<dyn ToolT>>,
    retry: Option<RetryConfig>,
) -> Result<ToolExecStep, TaskError> {
    let tool = tool_map
        .get(&call.function.name)
        .cloned()
        .ok_or_else(|| TaskError::StepFailed {
            step: step_name.clone(),
            source: StepError::Failed {
                step: step_name.clone(),
                reason: format!("unknown tool '{}'", call.function.name),
            },
        })?;
    let args: Value =
        serde_json::from_str(&call.function.arguments).unwrap_or_else(|_| Value::Null);
    Ok(ToolExecStep {
        name: step_name,
        inner: tool,
        args,
        retry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use autoagents::llm::FunctionCall;

    #[test]
    fn durable_chat_response_round_trips_serde() {
        let original = DurableChatResponse {
            text: Some("Thinking...".to_string()),
            tool_calls: Some(vec![ToolCall {
                id: "call_1".to_string(),
                call_type: "function".to_string(),
                function: FunctionCall {
                    name: "get_weather".to_string(),
                    arguments: r#"{"city":"Lisbon"}"#.to_string(),
                },
            }]),
            thinking: None,
            usage: None,
        };

        let json = serde_json::to_string(&original).expect("serialize");
        let decoded: DurableChatResponse = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, decoded);
        assert_eq!(decoded.text().as_deref(), Some("Thinking..."));
        assert_eq!(decoded.tool_calls().unwrap().len(), 1);
    }

    #[test]
    fn control_flow_slot_captures_first_signal_only() {
        let slot = ControlFlowSlot::new();
        assert!(!slot.is_captured());

        slot.capture(StepError::Failed {
            step: "first".to_string(),
            reason: "ctrl1".to_string(),
        });
        slot.capture(StepError::Failed {
            step: "second".to_string(),
            reason: "ctrl2".to_string(),
        });

        assert!(slot.is_captured());
        let taken = slot.take().expect("should have captured error");
        assert!(taken.to_string().contains("first"));
        assert!(!slot.is_captured());
    }
}
