//! Minimal Gemini 3.x `LLMProvider` that preserves `thoughtSignature` across
//! multi-turn tool calls by encoding it into `ToolCall::id`.
//!
//! Because `DurableChatResponse` serializes `ToolCall::id` into Zart's step
//! store, `thoughtSignature` survives process restarts and replays cleanly.

use async_trait::async_trait;
use autoagents::llm::chat::{
    ChatMessage, ChatProvider, ChatResponse, ChatRole, MessageType, StructuredOutputFormat, Tool,
    Usage,
};
use autoagents::llm::completion::{CompletionProvider, CompletionRequest, CompletionResponse};
use autoagents::llm::embedding::EmbeddingProvider;
use autoagents::llm::error::LLMError;
use autoagents::llm::models::ModelsProvider;
use autoagents::llm::{FunctionCall, LLMProvider, ToolCall};
use serde_json::{Value, json};
use zart_autoagents::DurableChatResponse;

const SIG_SEP: &str = "::ts::";

pub struct Gemini3Provider {
    api_key: String,
    model: String,
    client: reqwest::Client,
}

impl Gemini3Provider {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            client: reqwest::Client::new(),
        }
    }
}

fn encode_call_id(idx: usize, name: &str, sig: Option<&str>) -> String {
    match sig {
        Some(s) if !s.is_empty() => format!("call_{idx}_{name}{SIG_SEP}{s}"),
        _ => format!("call_{idx}_{name}"),
    }
}

fn decode_thought_sig(id: &str) -> Option<&str> {
    id.split_once(SIG_SEP).map(|(_, sig)| sig)
}

#[async_trait]
impl ChatProvider for Gemini3Provider {
    async fn chat_with_tools(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[Tool]>,
        _json_schema: Option<StructuredOutputFormat>,
    ) -> Result<Box<dyn ChatResponse>, LLMError> {
        let mut system_parts = Vec::new();
        let mut contents = Vec::new();

        for msg in messages {
            match (&msg.role, &msg.message_type) {
                (ChatRole::System, _) => {
                    if !msg.content.is_empty() {
                        system_parts.push(json!({ "text": msg.content }));
                    }
                }
                (ChatRole::Assistant, MessageType::ToolUse(calls)) => {
                    let mut parts = Vec::new();
                    if !msg.content.is_empty() {
                        parts.push(json!({ "text": msg.content }));
                    }
                    for call in calls {
                        let args: Value = serde_json::from_str(&call.function.arguments)
                            .unwrap_or_else(|_| json!({}));
                        let mut part = json!({
                            "functionCall": {
                                "name": call.function.name,
                                "args": args,
                            }
                        });
                        if let Some(sig) = decode_thought_sig(&call.id) {
                            part["thoughtSignature"] = json!(sig);
                        }
                        parts.push(part);
                    }
                    contents.push(json!({ "role": "model", "parts": parts }));
                }
                (_, MessageType::ToolResult(calls)) => {
                    let mut parts = Vec::new();
                    for call in calls {
                        let resp_val: Value = serde_json::from_str(&call.function.arguments)
                            .unwrap_or_else(|_| json!({ "output": call.function.arguments }));
                        let response_obj = if resp_val.is_object() {
                            resp_val
                        } else {
                            json!({ "result": resp_val })
                        };
                        parts.push(json!({
                            "functionResponse": {
                                "name": call.function.name,
                                "response": response_obj,
                            }
                        }));
                    }
                    contents.push(json!({ "role": "user", "parts": parts }));
                }
                (ChatRole::Assistant, _) => {
                    contents.push(json!({
                        "role": "model",
                        "parts": [{ "text": msg.content }]
                    }));
                }
                _ => {
                    contents.push(json!({
                        "role": "user",
                        "parts": [{ "text": msg.content }]
                    }));
                }
            }
        }

        let mut body = json!({ "contents": contents });
        if !system_parts.is_empty() {
            body["systemInstruction"] = json!({ "parts": system_parts });
        }
        if let Some(tools) = tools
            && !tools.is_empty()
        {
            let decls: Vec<Value> = tools
                .iter()
                .map(|t| {
                    json!({
                        "name": t.function.name,
                        "description": t.function.description,
                        "parameters": t.function.parameters,
                    })
                })
                .collect();
            body["tools"] = json!([{ "functionDeclarations": decls }]);
        }

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
            self.model
        );

        let resp = self
            .client
            .post(&url)
            .header("x-goog-api-key", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| LLMError::ProviderError(format!("Gemini HTTP error: {e}")))?;

        let status = resp.status();
        let raw: Value = resp
            .json()
            .await
            .map_err(|e| LLMError::ProviderError(format!("Gemini JSON parse error: {e}")))?;

        if !status.is_success() {
            return Err(LLMError::ProviderError(format!(
                "Gemini API error ({status}): {raw}"
            )));
        }

        let parts = raw["candidates"][0]["content"]["parts"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut text_acc = String::new();
        let mut tool_calls = Vec::new();

        for (idx, part) in parts.iter().enumerate() {
            if let Some(t) = part["text"].as_str() {
                text_acc.push_str(t);
            }
            if let Some(fc) = part.get("functionCall") {
                let name = fc["name"].as_str().unwrap_or_default().to_string();
                let args = serde_json::to_string(&fc["args"]).unwrap_or_else(|_| "{}".to_string());
                let sig = part["thoughtSignature"]
                    .as_str()
                    .or_else(|| part["thought_signature"].as_str());
                tool_calls.push(ToolCall {
                    id: encode_call_id(idx, &name, sig),
                    call_type: "function".to_string(),
                    function: FunctionCall {
                        name,
                        arguments: args,
                    },
                });
            }
        }

        let usage = raw.get("usageMetadata").map(|u| Usage {
            prompt_tokens: u["promptTokenCount"].as_u64().unwrap_or(0) as u32,
            completion_tokens: u["candidatesTokenCount"].as_u64().unwrap_or(0) as u32,
            total_tokens: u["totalTokenCount"].as_u64().unwrap_or(0) as u32,
            completion_tokens_details: None,
            prompt_tokens_details: None,
        });

        Ok(Box::new(DurableChatResponse {
            text: if text_acc.is_empty() {
                None
            } else {
                Some(text_acc)
            },
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            thinking: None,
            usage,
        }))
    }
}

#[async_trait]
impl CompletionProvider for Gemini3Provider {
    async fn complete(
        &self,
        _req: &CompletionRequest,
        _json_schema: Option<StructuredOutputFormat>,
    ) -> Result<CompletionResponse, LLMError> {
        Err(LLMError::ProviderError(
            "CompletionProvider not used".to_string(),
        ))
    }
}

#[async_trait]
impl EmbeddingProvider for Gemini3Provider {
    async fn embed(&self, _input: Vec<String>) -> Result<Vec<Vec<f32>>, LLMError> {
        Err(LLMError::ProviderError(
            "EmbeddingProvider not used".to_string(),
        ))
    }
}

#[async_trait]
impl ModelsProvider for Gemini3Provider {}

impl LLMProvider for Gemini3Provider {}
