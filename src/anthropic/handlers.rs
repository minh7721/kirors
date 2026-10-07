//! Anthropic API Handler 函数

use std::convert::Infallible;

use anyhow::Error;
use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::token;
use axum::{
    Json as JsonExtractor,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde_json::json;
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;

use super::converter::{ConversionError, convert_request};
use super::middleware::AppState;
use super::stream::{BufferedStreamContext, SseEvent, StreamContext};
use super::types::{CountTokensRequest, CountTokensResponse, ErrorResponse, Message, MessagesRequest, Model, ModelsResponse, OutputConfig, Thinking};
use super::websearch;

/// Maximum number of trim-retry cycles when context window is full
const MAX_CONTEXT_TRIM_RETRIES: usize = 3;

/// Check if an error indicates the context window is full
fn is_context_window_full(err: &Error) -> bool {
    err.to_string().contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD")
}

/// Remove the 2 oldest non-system messages from the message list,
/// preserving system-role messages and the last message (current user input).
///
/// Returns the number of messages removed (0 if not enough messages to trim).
fn trim_oldest_messages(messages: &mut Vec<Message>) -> usize {
    // Need at least 3 messages to trim (keep at least the last one)
    if messages.len() < 3 {
        return 0;
    }

    // Skip any leading system-role messages
    let start_idx = messages
        .iter()
        .position(|m| m.role != "system")
        .unwrap_or(messages.len());

    // Calculate how many non-system, non-last messages we can remove
    let max_removable = messages.len() - start_idx - 1; // preserve last message

    // Always remove a full pair (2) to make meaningful progress;
    // if we can't remove at least 2, don't bother trimming
    if max_removable < 2 {
        return 0;
    }

    messages.drain(start_idx..start_idx + 2);
    2
}

/// Proactive trim: nếu số message vượt ngưỡng thì cắt bớt trước khi gọi Kiro,
/// tránh tốn 1 request lỗi CONTENT_LENGTH_EXCEEDS_THRESHOLD.
fn proactive_trim_if_needed(messages: &mut Vec<Message>) -> usize {
    // Ngưỡng: > 30 messages thì chủ động cắt bớt 4 message cũ nhất
    // 30 messages ~ 15 turn, thường đã ~80k-120k token với system+tools lặp lại
    const PROACTIVE_THRESHOLD: usize = 30;
    const PROACTIVE_TRIM_COUNT: usize = 4;
    if messages.len() <= PROACTIVE_THRESHOLD {
        return 0;
    }
    let start_idx = messages
        .iter()
        .position(|m| m.role != "system")
        .unwrap_or(messages.len());
    let max_removable = messages.len() - start_idx - 1;
    if max_removable < PROACTIVE_TRIM_COUNT {
        return 0;
    }
    // Cắt 4 message (2 cặp) cũ nhất
    messages.drain(start_idx..start_idx + PROACTIVE_TRIM_COUNT);
    tracing::info!(
        "Proactive trim: removed {} oldest messages (remaining {})",
        PROACTIVE_TRIM_COUNT,
        messages.len()
    );
    PROACTIVE_TRIM_COUNT
}

/// Try to call the API, automatically trimming conversation history on context window overflow.
///
/// On `CONTENT_LENGTH_EXCEEDS_THRESHOLD` error, trims the oldest messages and retries
/// up to `MAX_CONTEXT_TRIM_RETRIES` times.
///
/// Returns `Ok((response, request_body, input_tokens))` on success,
/// or `Err(Response)` with the appropriate HTTP error.
async fn try_call_with_context_trim(
    provider: &std::sync::Arc<crate::kiro::provider::KiroProvider>,
    payload: &mut MessagesRequest,
    profile_arn: &Option<String>,
    is_stream: bool,
) -> Result<(reqwest::Response, String, i32), Response> {
    // Proactive: cắt bớt history trước khi gọi, tránh tốn 1 request lỗi
    let proactive = proactive_trim_if_needed(&mut payload.messages);
    let mut total_trimmed: usize = proactive as usize;
    // Auto Haiku routing: đọc từ config (nếu bật auto_haiku_routing trong config.json)
    let auto_haiku = provider.token_manager().config().auto_haiku_routing;
    for attempt in 0..=MAX_CONTEXT_TRIM_RETRIES {
        // Convert request (có thể tự route về Haiku nếu bật)
        let raw_conversion = if auto_haiku {
            crate::anthropic::converter::convert_request_with_auto_haiku(payload, true)
        } else {
            convert_request(payload)
        };
        let conversion_result = match raw_conversion {
            Ok(result) => result,
            Err(e) => {
                let (error_type, message) = match &e {
                    ConversionError::UnsupportedModel(model) => {
                        ("invalid_request_error", format!("模型不支持: {}", model))
                    }
                    ConversionError::EmptyMessages => {
                        ("invalid_request_error", "消息列表为空".to_string())
                    }
                };
                tracing::warn!("请求转换失败: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::new(error_type, message)),
                )
                    .into_response());
            }
        };

        // Build Kiro request
        let kiro_request = KiroRequest {
            conversation_state: conversion_result.conversation_state,
            profile_arn: profile_arn.clone(),
        };

        let request_body = match serde_json::to_string(&kiro_request) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!("序列化请求失败: {}", e);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new(
                        "internal_error",
                        format!("序列化请求失败: {}", e),
                    )),
                )
                    .into_response());
            }
        };

        // Estimate input tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        // Call API
        let api_result = if is_stream {
            provider.call_api_stream(&request_body).await
        } else {
            provider.call_api(&request_body).await
        };

        match api_result {
            Ok(response) => {
                if attempt > 0 {
                    tracing::info!(
                        total_retries = attempt,
                        total_messages_trimmed = total_trimmed,
                        remaining_messages = payload.messages.len(),
                        "Context window trim-retry succeeded"
                    );
                }
                return Ok((response, request_body, input_tokens));
            }
            Err(e) => {
                if is_context_window_full(&e) && attempt < MAX_CONTEXT_TRIM_RETRIES {
                    let removed = trim_oldest_messages(&mut payload.messages);
                    if removed == 0 {
                        tracing::warn!("Context window full but too few messages to trim, giving up");
                        return Err(map_provider_error(e));
                    }
                    total_trimmed += removed;
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = MAX_CONTEXT_TRIM_RETRIES,
                        messages_removed = removed,
                        total_trimmed = total_trimmed,
                        remaining_messages = payload.messages.len(),
                        "Context window full, trimming oldest messages and retrying"
                    );
                    continue;
                }
                return Err(map_provider_error(e));
            }
        }
    }

    // Should not reach here, but just in case
    unreachable!("try_call_with_context_trim loop should always return")
}

/// 将 KiroProvider 错误映射为 HTTP 响应
fn map_provider_error(err: Error) -> Response {
    let err_str = err.to_string();

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history, system prompt, or tools.",
            )),
        )
            .into_response();
    }

    // 单次输入太长（请求体本身超出上游限制）
    if err_str.contains("Input is too long") {
        tracing::warn!(error = %err, "上游拒绝请求：输入过长（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long. Reduce the size of your messages.",
            )),
        )
            .into_response();
    }
    tracing::error!("Kiro API 调用失败: {}", err);
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            format!("上游 API 调用失败: {}", err),
        )),
    )
        .into_response()
}

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models() -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let models = vec![
        Model {
            id: "claude-sonnet-4-5-20250929".to_string(),
            object: "model".to_string(),
            created: 1727568000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-sonnet-4-5-20250929-thinking".to_string(),
            object: "model".to_string(),
            created: 1727568000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-opus-4-5-20251101".to_string(),
            object: "model".to_string(),
            created: 1730419200,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-opus-4-5-20251101-thinking".to_string(),
            object: "model".to_string(),
            created: 1730419200,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-sonnet-4-6".to_string(),
            object: "model".to_string(),
            created: 1770314400,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.6".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-sonnet-4-6-thinking".to_string(),
            object: "model".to_string(),
            created: 1770314400,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.6 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-opus-4-6".to_string(),
            object: "model".to_string(),
            created: 1770314400,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.6".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-opus-4-6-thinking".to_string(),
            object: "model".to_string(),
            created: 1770314400,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.6 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-haiku-4-5-20251001".to_string(),
            object: "model".to_string(),
            created: 1727740800,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Haiku 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        Model {
            id: "claude-haiku-4-5-20251001-thinking".to_string(),
            object: "model".to_string(),
            created: 1727740800,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Haiku 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 32000,
        },
        // --- Claude 5 series ---
        Model {
            id: "claude-sonnet-5".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-5-5".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-5-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-5".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-5-5".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-5-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1790000000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
    ];

    Json(ModelsResponse {
        object: "list".to_string(),
        data: models,
    })
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages request"
    );
    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求 + 调用 API（支持上下文窗口满时自动裁剪重试）
    let is_stream = payload.stream;
    let (response, _request_body, input_tokens) =
        match try_call_with_context_trim(&provider, &mut payload, &state.profile_arn, is_stream).await {
            Ok(result) => result,
            Err(err_response) => return err_response,
        };

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    if is_stream {
        // 流式响应
        handle_stream_request(
            response,
            &payload.model,
            input_tokens,
            thinking_enabled,
        )
        .await
    } else {
        // 非流式响应
        handle_non_stream_request(response, &payload.model, input_tokens, thinking_enabled)
            .await
    }
}

/// 处理流式请求
async fn handle_stream_request(
    response: reqwest::Response,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
) -> Response {
    // 创建流处理上下文
    let mut ctx = StreamContext::new_with_thinking(model, input_tokens, thinking_enabled);

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流
    let stream = create_sse_stream(response, ctx, initial_events);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Ping 事件间隔（25秒）
const PING_INTERVAL_SECS: u64 = 25;

/// 创建 ping 事件的 SSE 字符串
fn create_ping_sse() -> Bytes {
    Bytes::from("event: ping\ndata: {\"type\": \"ping\"}\n\n")
}

/// 创建 SSE 事件流
fn create_sse_stream(
    response: reqwest::Response,
    ctx: StreamContext,
    initial_events: Vec<SseEvent>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 先发送初始事件
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.bytes_stream();

    let processing_stream = stream::unfold(
        (body_stream, ctx, EventStreamDecoder::new(), false, interval(Duration::from_secs(PING_INTERVAL_SECS))),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval)| async move {
            if finished {
                return None;
            }

            // 使用 select! 同时等待数据和 ping 定时器
            tokio::select! {
                // 处理数据流
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            // 解码事件
                            if let Err(e) = decoder.feed(&chunk) {
                                tracing::warn!("缓冲区溢出: {}", e);
                            }

                            let mut events = Vec::new();
                            for result in decoder.decode_iter() {
                                match result {
                                    Ok(frame) => {
                                        if let Ok(event) = Event::from_frame(frame) {
                                            let sse_events = ctx.process_kiro_event(&event);
                                            events.extend(sse_events);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("解码事件失败: {}", e);
                                    }
                                }
                            }

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)))
                        }
                        Some(Err(e)) => {
                            tracing::error!("读取响应流失败: {}", e);
                            // 发送最终事件并结束
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)))
                        }
                        None => {
                            // 流结束，发送最终事件
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick() => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)))
                }
            }
        },
    )
    .flatten();

    initial_stream.chain(processing_stream)
}

/// 上下文窗口大小（200k tokens）
const CONTEXT_WINDOW_SIZE: i32 = 200_000;

/// 处理非流式请求
async fn handle_non_stream_request(
    response: reqwest::Response,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
) -> Response {
    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取响应体失败: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    // 解析事件流
    let mut decoder = EventStreamDecoder::new();
    if let Err(e) = decoder.feed(&body_bytes) {
        tracing::warn!("缓冲区溢出: {}", e);
    }

    let mut text_content = String::new();
    // 原生 reasoningContentEvent 的思考内容（5.x 模型）
    let mut thinking_content = String::new();
    let mut tool_uses: Vec<serde_json::Value> = Vec::new();
    let mut has_tool_use = false;
    let mut stop_reason = "end_turn".to_string();
    // 从 contextUsageEvent 计算的实际输入 tokens
    let mut context_input_tokens: Option<i32> = None;

    // 收集工具调用的增量 JSON
    let mut tool_json_buffers: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    for result in decoder.decode_iter() {
        match result {
            Ok(frame) => {
                if let Ok(event) = Event::from_frame(frame) {
                    match event {
                        Event::AssistantResponse(resp) => {
                            text_content.push_str(&resp.content);
                        }
                        Event::ReasoningContent(reasoning) => {
                            if thinking_enabled {
                                thinking_content.push_str(&reasoning.text);
                            }
                        }
                        Event::ToolUse(tool_use) => {
                            has_tool_use = true;

                            // 累积工具的 JSON 输入
                            let buffer = tool_json_buffers
                                .entry(tool_use.tool_use_id.clone())
                                .or_insert_with(String::new);
                            buffer.push_str(&tool_use.input);

                            // 如果是完整的工具调用，添加到列表
                            if tool_use.stop {
                                let input: serde_json::Value = if buffer.is_empty() {
                                    serde_json::json!({})
                                } else {
                                    serde_json::from_str(buffer)
                                        .unwrap_or_else(|e| {
                                            tracing::warn!(
                                                "工具输入 JSON 解析失败: {}, tool_use_id: {}",
                                                e, tool_use.tool_use_id
                                            );
                                            serde_json::json!({})
                                        })
                                };

                                tool_uses.push(json!({
                                    "type": "tool_use",
                                    "id": tool_use.tool_use_id,
                                    "name": tool_use.name,
                                    "input": input
                                }));
                            }
                        }
                        Event::ContextUsage(context_usage) => {
                            // 从上下文使用百分比计算实际的 input_tokens
                            // 公式: percentage * 200000 / 100 = percentage * 2000
                            let actual_input_tokens = (context_usage.context_usage_percentage
                                * (CONTEXT_WINDOW_SIZE as f64)
                                / 100.0)
                                as i32;
                            context_input_tokens = Some(actual_input_tokens);
                            // 上下文使用量达到 100% 时，设置 stop_reason 为 model_context_window_exceeded
                            if context_usage.context_usage_percentage >= 100.0 {
                                stop_reason = "model_context_window_exceeded".to_string();
                            }
                            tracing::debug!(
                                "收到 contextUsageEvent: {}%, 计算 input_tokens: {}",
                                context_usage.context_usage_percentage,
                                actual_input_tokens
                            );
                        }
                        Event::Exception { exception_type, .. } => {
                            if exception_type == "ContentLengthExceededException" {
                                stop_reason = "max_tokens".to_string();
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!("解码事件失败: {}", e);
            }
        }
    }

    // 确定 stop_reason
    if has_tool_use && stop_reason == "end_turn" {
        stop_reason = "tool_use".to_string();
    }

    // 构建响应内容
    let mut content: Vec<serde_json::Value> = Vec::new();

    if !thinking_content.is_empty() {
        content.push(json!({
            "type": "thinking",
            "thinking": thinking_content
        }));
    }

    if !text_content.is_empty() {
        content.push(json!({
            "type": "text",
            "text": text_content
        }));
    }

    content.extend(tool_uses);

    // 估算输出 tokens
    let output_tokens = token::estimate_output_tokens(&content);

    // 使用从 contextUsageEvent 计算的 input_tokens，如果没有则使用估算值
    let final_input_tokens = context_input_tokens.unwrap_or(input_tokens);

    // Tính cache tokens để Claude Code hiển thị đúng (Kiro không có cache thật,
    // nhưng báo đúng giúp client không tưởng nhầm và gửi lại full prompt)
    // cache_read/cache_creation đều 0 vì Kiro tính theo AGENTIC_REQUEST, không theo token
    let response_body = json!({
        "id": format!("msg_{}", Uuid::new_v4().to_string().replace('-', "")),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": final_input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0
        }
    });

    (StatusCode::OK, Json(response_body)).into_response()
}

/// 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
///
/// - Opus 4.6/5/5.5：覆写为 adaptive 类型
/// - 其他模型：覆写为 enabled 类型
/// - budget_tokens 固定为 20000
fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") {
        return;
    }

    let is_opus_adaptive = model_lower.contains("opus")
        && (model_lower.contains("4-6")
            || model_lower.contains("4.6")
            || model_lower.contains("5-5")
            || model_lower.contains("5.5")
            || model_lower.contains("opus-5")
            || model_lower.contains("opus 5"));

    let thinking_type = if is_opus_adaptive {
        "adaptive"
    } else {
        "enabled"
    };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        budget_tokens: 20000,
    });

    if is_opus_adaptive {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
        });
    }
}

/// POST /v1/messages/count_tokens
///
/// 计算消息的 token 数量
pub async fn count_tokens(
    JsonExtractor(payload): JsonExtractor<CountTokensRequest>,
) -> impl IntoResponse {
    tracing::info!(
        model = %payload.model,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1) as i32,
    })
}

/// POST /cc/v1/messages
///
/// Claude Code 兼容端点，与 /v1/messages 的区别在于：
/// - 流式响应会等待 kiro 端返回 contextUsageEvent 后再发送 message_start
/// - message_start 中的 input_tokens 是从 contextUsageEvent 计算的准确值
pub async fn post_messages_cc(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求 + 调用 API（支持上下文窗口满时自动裁剪重试）
    let is_stream = payload.stream;
    let (response, _request_body, input_tokens) =
        match try_call_with_context_trim(&provider, &mut payload, &state.profile_arn, is_stream).await {
            Ok(result) => result,
            Err(err_response) => return err_response,
        };

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    if is_stream {
        // 流式响应（缓冲模式）
        handle_stream_request_buffered(
            response,
            &payload.model,
            input_tokens,
            thinking_enabled,
        )
        .await
    } else {
        // 非流式响应（复用现有逻辑，已经使用正确的 input_tokens）
        handle_non_stream_request(response, &payload.model, input_tokens, thinking_enabled)
            .await
    }
}

/// 处理流式请求（缓冲版本）
///
/// 与 `handle_stream_request` 不同，此函数会缓冲所有事件直到流结束，
/// 然后用从 contextUsageEvent 计算的正确 input_tokens 生成 message_start 事件。
async fn handle_stream_request_buffered(
    response: reqwest::Response,
    model: &str,
    estimated_input_tokens: i32,
    thinking_enabled: bool,
) -> Response {
    // 创建缓冲流处理上下文
    let ctx = BufferedStreamContext::new(model, estimated_input_tokens, thinking_enabled);

    // 创建缓冲 SSE 流
    let stream = create_buffered_sse_stream(response, ctx);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// 创建缓冲 SSE 事件流
///
/// 工作流程：
/// 1. 等待上游流完成，期间只发送 ping 保活信号
/// 2. 使用 StreamContext 的事件处理逻辑处理所有 Kiro 事件，结果缓存
/// 3. 流结束后，用正确的 input_tokens 更正 message_start 事件
/// 4. 一次性发送所有事件
fn create_buffered_sse_stream(
    response: reqwest::Response,
    ctx: BufferedStreamContext,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let body_stream = response.bytes_stream();

    stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            interval(Duration::from_secs(PING_INTERVAL_SECS)),
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval)| async move {
            if finished {
                return None;
            }

            loop {
                tokio::select! {
                    // 使用 biased 模式，优先检查 ping 定时器
                    // 避免在上游 chunk 密集时 ping 被"饿死"
                    biased;

                    // 优先检查 ping 保活（等待期间唯一发送的数据）
                    _ = ping_interval.tick() => {
                        tracing::trace!("发送 ping 保活事件（缓冲模式）");
                        let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                        return Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)));
                    }

                    // 然后处理数据流
                    chunk_result = body_stream.next() => {
                        match chunk_result {
                            Some(Ok(chunk)) => {
                                // 解码事件
                                if let Err(e) = decoder.feed(&chunk) {
                                    tracing::warn!("缓冲区溢出: {}", e);
                                }

                                for result in decoder.decode_iter() {
                                    match result {
                                        Ok(frame) => {
                                            if let Ok(event) = Event::from_frame(frame) {
                                                // 缓冲事件（复用 StreamContext 的处理逻辑）
                                                ctx.process_and_buffer(&event);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("解码事件失败: {}", e);
                                        }
                                    }
                                }
                                // 继续读取下一个 chunk，不发送任何数据
                            }
                            Some(Err(e)) => {
                                tracing::error!("读取响应流失败: {}", e);
                                // 发生错误，完成处理并返回所有事件
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)));
                            }
                            None => {
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)));
                            }
                        }
                    }
                }
            }
        },
    )
    .flatten()
}
