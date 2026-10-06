# Hướng dẫn tiết kiệm credit Kiro K12 Edu (kirors)

> Kiro K12 Edu tính phí theo **số request `AGENTIC_REQUEST`** (mỗi lần gọi `generateAssistantResponse` = 1 credit), không tính theo token. Vì vậy `cachePoint` của Bedrock không giúp tiết kiệm K12. Cách duy nhất là **giảm số request và giảm số token mỗi request**.

---

## Tầng 1 — Chỉ đổi config, không sửa code (giảm 40-50% ngay)

### 1. Bật `balanced` nếu có ≥2 credentials
`config/config.json`:
```json
{
  "loadBalancingMode": "balanced"
}
```
`priority` dồn hết vào 1 acc nên nhanh hết 1000 credit. `balanced` chia đều theo `success_count` nên ít bị `429`.

### 2. Ép model rẻ cho task vặt
Trong `config/config.json` bật auto routing:
```json
{
  "autoHaikuRouting": true
}
```
Khi bật, request **không có `thinking`** và không có `tool_choice` phức tạp sẽ tự động map `opus/sonnet → haiku-4.5`. Haiku và Sonnet với Kiro đều tính 1 credit như nhau, nhưng Haiku ít bị `429 high traffic` nên ít retry hơn. Task Read/Grep/Bash chất lượng như nhau.

Nếu muốn tự kiểm soát ở phía client, thêm vào `~/.claude/settings.json`:
```json
{
  "model": "claude-haiku-4-5-20251001"
}
```

### 3. Giảm `max_tokens` và tắt thinking khi không cần
- Để `max_tokens = 8192-12000` thay vì 32000 mặc định.
- Chỉ bật thinking cho `opus-4-6/5`, Sonnet/Haiku để `thinking: {type: "disabled"}`.

### 4. Hạn chế agent loop hoang phí (quan trọng nhất, 0 dòng code)
Thêm vào `CLAUDE.md` của dự án:
```
- Khi Read/Grep xong, tổng hợp rồi mới Write, không Read lại file vừa Grep
- Ưu tiên Grep thay vì Read cả thư mục
- Batch tool call: gộp 3-4 Read thành 1 turn thay vì 3 turn riêng
- Mỗi task cố gắng hoàn thành trong < 10 turn
```
Mỗi turn = 1 credit K12. Giảm từ 25 turn → 12 turn là tiết kiệm 50%.

---

## Tầng 2 — Đã code sẵn trong kirors (giảm thêm 20-30%)

Các thay đổi đã được commit vào branch hiện tại, chỉ cần rebuild là có hiệu lực:

| Thay đổi | File | Hiệu quả |
|---|---|---|
| Parse `cache_control` từ Claude Code (`SystemMessage`, `Tool`, `ContentBlock`) | `types.rs` | Không còn làm mất thông tin cache của client |
| Lọc tool không dùng (chỉ giữ tool có `cache_control` + tool đã dùng trong history + 7 tool cốt lõi) | `converter.rs:convert_tools_filtered` | Giảm 6-10k token/request khi Claude gửi 35-40 tools |
| Idempotent `SYSTEM_CHUNKED_POLICY` (không nối chuỗi 2 lần) | `converter.rs:build_history` | Giảm lặp system prompt |
| Proactive trim khi >30 messages (cắt 4 message cũ nhất trước khi gọi Kiro) | `handlers.rs:proactive_trim_if_needed` | Tránh tốn 1 request lỗi `CONTENT_LENGTH_EXCEEDS_THRESHOLD` rồi mới retry |
| Trả `usage.cache_creation/read_input_tokens: 0` đúng chuẩn | `handlers.rs`, `stream.rs` | Claude Code hiển thị đúng, không gửi lại full prompt vô ích |
| Auto Haiku routing (opt-in qua `autoHaikuRouting`) | `config.rs`, `converter.rs`, `handlers.rs` | Giảm retry Opus trên K12 FREE/K12 |

### Cách bật sau khi rebuild
```bash
docker compose build kiro-rs && docker compose up -d kiro-rs
# hoặc nếu chạy binary:
cargo build --release && ./target/release/kiro-rs -c config/config.json
```

---

## Kiểm tra hiệu quả
- Xem `config/kiro_stats.json` (success_count/last_used_at mỗi credential)
- Xem `config/kiro_balance_cache.json` hoặc Admin UI `/admin` → Balance
- Log sẽ ghi `Proactive trim: removed 4 oldest messages` khi trigger
- Log tool filtering: khi request có >12 tools và có `cache_control`, số tool gửi lên Kiro sẽ ít hơn số tool nhận từ Claude Code
