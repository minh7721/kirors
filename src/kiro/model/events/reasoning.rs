//! 推理内容事件
//!
//! 处理 reasoningContentEvent 类型的事件（Claude 5.x 等原生 thinking 模型）

use serde::Deserialize;

use crate::kiro::parser::error::ParseResult;
use crate::kiro::parser::frame::Frame;

use super::base::EventPayload;

/// 推理内容事件
///
/// 原生 thinking 模型（如 claude-opus-5.5 / claude-sonnet-5）不会在文本中输出
/// `<thinking>` 标签，而是通过此事件流式返回思考内容。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningContentEvent {
    /// 思考内容片段
    #[serde(default)]
    pub text: String,
}

impl EventPayload for ReasoningContentEvent {
    fn from_frame(frame: &Frame) -> ParseResult<Self> {
        frame.payload_as_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let event: ReasoningContentEvent = serde_json::from_str(r#"{"text":"Tôi cần"}"#).unwrap();
        assert_eq!(event.text, "Tôi cần");
    }

    #[test]
    fn test_deserialize_missing_text() {
        let event: ReasoningContentEvent = serde_json::from_str("{}").unwrap();
        assert!(event.text.is_empty());
    }
}
