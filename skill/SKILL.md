---
name: nonebot-bridge
description: |
  Adapter for chatting in third-party group/private chats via the nonebot-plugin-hermes-adapter bridge.
  Activates when you see `mode: reactive` in a system message, or a tool result mentions
  `nonebot-bridge`. Use this skill to push messages to a chat group, list active groups, or pull context.

tools:
  - push_message
  - recall_message
  - list_active_sessions
  - get_recent_messages
  - get_message_images
---

# nonebot-bridge — chat adapter (M1)

You are wired to a nonebot2 process that forwards group / private chat messages to you and
allows you to send messages back via the `push_message` tool.

## Modes

- **reactive** (M1): A user invited you into an active group conversation. Follow the
  current system prompt's interface-specific contract; private chats use their own
  addressing rules and never need mention markers.
- **Runs:** Send visible acknowledgements, progress and results as complete two-line
  decision envelopes at the moment they are needed. Continue actual tool work in the
  same run; several consecutive envelopes are allowed. Do not use tool pushes in an
  active run. Ordinary narration and bare final text are not visible chat replies.
- **completions:** Use `push_message` for visible mid-turn progress. The final answer
  must still be a decision envelope, not a tool push. For an unfinished long task,
  send its acknowledgement with the continuation control described below.

## Tools

### `push_message(adapter, group_id, text, image_urls?, reply_to?)`

Send one message into a group. Constraints:
- `reply_to` optionally quotes a same-chat message by its plugin `m:id`, for example
  `m:123`; never pass a NapCat/OneBot message id. Invalid or unavailable mappings degrade
  to an ordinary unquoted message and are logged. A resolved group Reply automatically
  adds `At(quoted sender) + one space` after the Reply segment; private replies do not.
- Insert `[CQ:at,qq=数字]` directly in `text` or decision `body` for ordered
  mentions. Multiple markers and arbitrary positions are supported. Other CQ codes and
  `@all` are not enabled.
- `image_urls` accepts **only** `http(s)://` and `data:` URLs. A path on this host
  (`/root/.hermes/cache/images/…`) is not reachable from the bot process and is
  **not** delivered — the result reports it in `skipped_images` with a `warning`,
  and the text is still sent. Do not retry the same path: to send a locally
  generated image, return a decision envelope whose `body` carries a
  `MEDIA:<absolute path>` tag instead (see "Sending images" below).
- The (adapter, group_id) must currently be in an **active reactive session**. If not,
  returns `ok=false` with `error="no active reactive session"`. Do not retry — the user
  has not invited you in.
- The (adapter, group_id) must be **known** (the bot has seen at least one message in that
  group since process start). A fresh nonebot restart causes `error="unknown target"` until
  members talk again.

### `recall_message(adapter, message_id, group_id?, user_id?)`

Withdraw one message previously sent by this bot. Call it only when the user explicitly
asks for a recall and identifies the target by quoting it or describing its content.
Resolve that target to one unique visible `m:id` before calling; the tool does not search
message content, guess among candidates, or default to the latest message. If the target
is ambiguous, do not call it.

- `message_id` is the plugin logical id in strict `m:<positive integer>` form, for example
  `m:123`; never pass a NapCat/OneBot message id.
- Give exactly one target: `group_id` for a group or `user_id` for a private chat. It must
  be the chat that contains the message. Group calls require an active reactive session;
  private calls follow the same private allowlist as proactive private delivery.
- Only a message whose recorded sender is the current bot can be recalled. Missing old
  platform mappings, cross-chat ids, other users' messages, and other bots' messages are
  rejected locally without calling the platform.
- `error_kind="local_rejected"` means the target or permission check failed;
  `"platform_rejected"` means QQ/NapCat refused the operation (time limit, permission,
  or message state may apply); `"unconfirmed"` means the response was lost or timed out
  and the message may already have been withdrawn. Do not blindly retry platform refusals
  or unconfirmed outcomes.

### `list_active_sessions(adapter?)`

List all groups currently in active reactive state. Returns sessions with `expires_at`
and `triggered_by`. Use to decide whether a delayed `push_message` is still welcome.

### `get_recent_messages(adapter, group_id, limit?, before_ts?)`

Pull the latest `limit` messages from a group buffer (capped at 50). Prefer the
`<recent_messages>` block already inlined in your reactive prompt, but note that on
follow-up turns that block may only carry the not-yet-pushed increment (a block header
line says so), not the full buffer — if the message you're looking for isn't in it, page
back with this tool before concluding it doesn't exist. It's still context-expensive, so
use it deliberately, not by default.

Each returned message carries:
- `id` — DB primary key, stable across turns; pair with `get_message_images` to fetch images
- `image_count` — number of regular images attached (0 = none)
- `sticker_count` — number of QQ stickers (收藏表情) attached; recallable the same way
- Note: `image_count` / `sticker_count` do **not** include quoted images or forward
  images — those appear in the line's media manifest (e.g. `(引用#2 转发#3)`) instead.
  The counter gap is a known view limitation (registered); don't take these counters as
  the full media picture.
- `text`, `user`, `ts`, `is_bot` — as before

### `get_background_tasks(adapter, group_id? | user_id?)`

Read-only view of the background work really registered for this conversation: subagent /
Kanban results the plugin still has to report back here. `task_state` is the upstream task
state, `report_state` the plugin's own delivery progress, `summary` a one-line result
excerpt. An empty list means nothing is registered — never claim a background task from
memory, and never invent a task note; the plugin wakes you when the result is ready.

### `get_message_images(message_ids, adapter?, group_id?)`

Fetch image bytes for up to 4 message ids in a single call. Returns a content array
mixing a JSON header (per-image metadata, including `kind`) with `TextContent` markers
+ `ImageContent` base64 blocks. The image content flows through Hermes's multimodal
injection pipeline, so on your next LLM turn you'll **actually see** the images.

Stickers are stored and recalled through the exact same pipeline. Their marker reads
`[sticker for m:<id> idx=N]` instead of `[image for ...]` — treat sticker bytes as a
mood/tone signal (same rule as inline stickers: don't describe or fact-check the meme
itself unless the user asks). The per-image `kind` field in the JSON header is one of
`"image"`, `"sticker"`, `"quote"` (an image quoted by the message) or `"forward_image"`
(an image inside a forwarded block the message shared).

Each per-image result has `available: true|false`. When unavailable, `reason` is one of:
- `cache_miss` — image was never fetched (URL expired before download) or has been
  evicted from the local cache; tell the user the image is gone, request a resend
- `not_found` — the message_id is not in the DB anymore (past 30-day retention or
  hard row cap)
- `compress_failed` — image exceeded the fetch size gate and both compression ladder
  steps still overshot; ask the user to send a smaller/compressed version
- `cap_exceeded` — your call totaled more than 25 MB; retry with fewer ids

Oversized images are compressed automatically (fetch ladder); animated images return
multiple frames (up to the configured fetch frame cap) with per-frame markers.

## Output contract (Runs)

On Runs your final reply is a one-line JSON body object:

```
{"body": "reply text"}
```

A non-empty body is sent to the chat as-is; an empty body means silence. You may emit
several of these back-to-back (a blank line between them is allowed); each one is judged
and sent on its own. Ordinary replies need no control call at all — do not spend a turn
just to express control.

When a reply actually needs a control, register it with the `submit_control` tool first
and then emit the body decision:

- `reply_to` — quote one message of this conversation by its plugin `m:id` (`m:123`); only
  that shape is accepted, never a NapCat id or a Hermes transcript id
- `should_exit_active` — `true` = leave the active window once this reply is sent
- `pin_anchor` — commission judgement turns only; `true` pins the current request
- `release_anchor` / `answer_anchor` — commissioner user_id or `"all"` (unpin / mark answered)
- `anchor_merge` — starter user_id of a pinned task this message belongs to

One tool call registers one control, consumed by the next explicit body (an empty body
counts as a decision); several registrations are consumed in order, and with none
registered the defaults apply. The tool only registers: it never sends a message and never
leaves the window or pins by itself — the body still does the sending and the existing
post-send step still applies the controls. If the control is lost (tool failure, broken
stream, restart) the body is still sent with default controls.

On completions the input is still the two-line envelope (control line + body line) and
`not_final: true` is available there: it means this reply is NOT your final result but a
task acknowledgement — the plugin resends it as-is and immediately fires a continue turn.
Runs continues tool work in the same run instead of requesting a plugin continuation turn.

Mentions live inside `body`: write `[CQ:at,qq=12345]` exactly where each strong
notification belongs. Multiple people and arbitrary positions are supported; the plugin
preserves segment order and does not prepend or infer mentions. Omit markers for ordinary
replies, and do not use other CQ codes or `@all`.

`reply_to` independently quotes one specific message: set it to the plugin `m:id` already
shown in history, such as `m:123`. Never substitute a NapCat message id or Hermes transcript
row id. Unknown, legacy, malformed or cross-chat mappings are ignored with a log rather than
being sent raw.

Task anchors: when the input contains a `<pinned_tasks>` block, those are pinned
commission anchors (📌 + attribution + age), NOT latest messages — they keep the
original request visible after context trimming. Anchors are pinned only when a
commission judgement turn explicitly sets `pin_anchor=true`; don't pin jokes or
one-off chatter. When an anchored task is finished
or the topic has drifted away, set `release_anchor` to the commissioner's user_id
(or "all") to unpin. When your reply answers an anchored commission, set
`answer_anchor` to the commissioner's user_id (or "all") to mark it answered — an
answered guest anchor yields its slot to new commissions sooner. Only ids currently
pinned are accepted; unknown ids are ignored and echoed back next turn as a plugin
note, so fix it then.

Anchors are per-task, not per-person: participants share equal footing. When the
current message belongs to a pinned task (a follow-up spec change, an acceptance
reply), set `anchor_merge` to that task's starter user_id to join its participants
instead of pinning a new anchor; quoting the task's original message auto-merges
too. Mid-turn you may receive steered messages from stakeholders ([user=…] lines):
handle them inline; if one is a brand-new independent commission, acknowledge it in
your reply or ask them to re-send later — steered messages never create anchors.

Blacklist: when runtime_state has `sender_blacklisted: true`, the current
sender is on the soft blacklist — default to an empty body even if they @ you.
Exception: an active anchor or a recent human message explicitly commissioned
interaction with that account (e.g. "你去跟它说 X"); resume normal conversation for
the commission's lifetime only.

An empty body sends nothing — that is the **correct** behavior for
"this conversation isn't about me, I'll stay quiet." Staying silent ≠ leaving — keep
`should_exit_active=false` so you still hear the next message in the active window.

### Your own account

`<runtime_state>` carries `you: [user=<platform nickname> id=<your account id>]` — that is
**your** account on the chat platform. Mentions arrive inlined into the message text as bare
`@<id>` placeholders, so `@<your id>` means someone is @-ing **you**, even when the number
looks unfamiliar or the platform nickname differs from your persona name. Lines in
`<recent_messages>` tagged `[bot]` with that same `id=` are your own past messages, not
another bot's.

### Addressing is already settled for you

When `<runtime_state>` carries `addressed_to_you: true`, the plugin has established from the
platform itself that this message is aimed at you — a real @-mention of your account, a quote
of one of your messages, or a platform event targeting you (e.g. a poke).
Ownership is not yours to re-litigate: never answer with
"maybe they meant someone else" or "not sure this was for me". Staying silent is still fine
when you have nothing to add or already answered the same question; doubting the addressee
is not one of the reasons. Without that line, ownership is genuinely unknown and the rules
above apply.

### Exit threshold (`should_exit_active`)

Registering `should_exit_active: true` with `submit_control` closes the active window after
that reply is sent (on completions the same flag lives in the envelope control line). Once
closed, non-@ messages are dropped before you ever see them. Set the bar **high**:

- Set `true` only when:
  - User explicitly says goodbye / thanks that's enough / never mind / 不用了.
  - You completed the last explicit request **and** the most recent message is clearly
    unrelated to you.
  - Group topic has fully shifted away and stayed off-topic for 3+ messages.
- Keep `false` for:
  - User's verbal thinking (“我想想”, "let me see", hesitation, pauses).
  - Brief lulls or off-topic banter mid-conversation.
  - Any message you're unsure about — silence is cheap, premature exit is not.

### Honesty about attempts

When the user asks you to do something (look up data, search, fetch info):

- **Try first.** If your tools can actually attempt it, run them and put the real result
  into the body line.
- **If the attempt fails or the request is genuinely beyond your reach,** say so plainly:
  "I couldn't find that — try X instead" / “我这查不到 X，可以用 Y 看看”。
- **Do not** promise work without an actual execution path. For a long accepted task,
  acknowledge it first using the current interface's protocol, then actually execute.
  A short task needs only its result. If execution is blocked, state the blocker.

## Sending images (outbound media)

To put an image in the chat, write a `MEDIA:<absolute path>` tag **inside** the field the
plugin actually delivers:

- reactive mode → inside the body line of your decision envelope.
- `push_message` → pass an http(s) URL in `image_urls` (the `text` field's MEDIA tag is
  NOT processed there).

For image files the plugin inlines the bytes itself and the image ships as a real image
message. Constraints:

- **Absolute path only** (`MEDIA:/home/user/out.png`), extension one of
  `.png .jpg .jpeg .gif .webp .bmp`, file **≤ 5 MB**, and the path must sit inside the
  whitelist roots (the Hermes media cache dirs such as `~/.hermes/cache/scratch`).
- A **non-image** local file (zip, txt, …) is delivered as a group/private file upload
  instead — the `[生成了文件: <name>]` placeholder in your text is its caption, and the
  file card follows. That is the intended file-sending path, not a failure.
- A path that fails the whitelist check is not delivered at all: the placeholder remains
  with no upload behind it, so **say in your reply text that the file couldn't be sent**
  rather than acting as though it went through.
- **Never hand-write a `data:` / base64 URL yourself**, and never put the `MEDIA:` tag
  outside the JSON envelope — content outside the envelope is dropped, not sent.
- If a reply is cut off mid-image the user gets a `[图片传输不完整]` placeholder. Keep
  the body short when it carries an image; the image itself costs you no output tokens,
  but a long reply around it raises the odds of truncation.

## Historical media recall

When the user refers to a past image (e.g. "上图", "这图", "刚才那张", "他刚发的"),
the `<recent_messages>` block shows `[图片]` placeholders but not the actual image.
Media addressing (2026-10-15): every addressable medium carries a per-image label —
history lines append a compact manifest like `(图#0 #1 表情#2)` (idx runs continuous
per message: images → stickers → quoted images → forward images); inlined image parts
are preceded by a full label `[图 m:22371#0]`; quoted images appear as `(引用图#k)`
inside the quote prefix, forward images as `[转发图#x]` inside forwarded blocks
(registered but never inlined — fetch on demand). The label's m:id#idx is the exact
address for recall.
To see the image, follow this two-step protocol:

1. **Identify the message.** Each line in `<recent_messages>` starts with `[m:<id>]` —
   that id is the DB primary key, stable across turns. Heuristics:
   - "上图" / "这图" / "刚才那张" → most recent line where the message has an image
   - "我刚发的" → most recent image-bearing message from the current speaker
   - "他刚发的" → most recent image-bearing message from the user named in context
2. **Fetch the bytes.** Call `get_message_images(message_ids=[<id>])`. The returned
   ImageContent blocks become real visual input on your next LLM turn. To fetch only
   specific images of a multi-image message, pass the optional `idx=[...]` filter
   (numbers from the line's media manifest, e.g. `(图#0 #1)` → idx 0/1).

On follow-up turns `<recent_messages>` only carries the not-yet-pushed increment, so the
image-bearing line the user means may not appear in it at all. In that case, call
`get_recent_messages` first to locate the `[m:<id>]`, then `get_message_images`. Don't
conclude "no image found" from the increment block alone.

The `[m:<id>]` id is **stable** across turns — the same image always has the same label,
unlike positional schemes (#1, #2, …) which shift when new messages arrive. Use it to
anchor cross-turn references ("I analyzed m:1234 last turn, user is asking about it again").

The `<current_message>` block carries `[m:<id>]` when the triggering message is
already persisted (the normal case — perception stores before the turn builds),
plus the same media manifest suffix. Rare fallback (row not found) = no prefix.

### When to skip the tool

- The user did not reference an image — don't fetch (token cost).
- `image_count == 0` on every recent message (and no media manifest on any of them) —
  tell the user directly, nothing to look at.
- Identical image already fetched this turn — reuse the previous result.

## Forwarded message blocks

Group messages may contain `<forwarded_messages count="N">...</forwarded_messages>`
blocks, or self-closing summary / fetch-failed variants of the same tag. These blocks
are transcript snapshots of a "merged forward" message the user shared:

- Images **inside** the block are numbered as `[转发图#x]` and **can** be recalled:
  call `get_message_images` with the owning message's `id` (its `[m:<id>]` line prefix)
  plus the `idx` shown for it in the line's media manifest (e.g. `(转发#3)` → `idx=[3]`).
  Forward images are registered for numbered recall but never inlined by default —
  fetch on demand.
- `[语音] [视频] [文件:...]` placeholders **inside** the block have **no** retrievable
  URL — do not call `get_message_images` for those.
- Placeholders **outside** any forwarded block follow the normal retrieval rules.
- `status="fetch_failed"` means a merged-forward message exists but the plugin could
  not expand it. Treat it as "the user shared a chat log, contents unknown."
- The self-closing `preview="..."` form is a compact summary of a forwarded block
  that appeared in an earlier turn. Contents are deliberately minimised; do not ask
  the user to re-share for more detail.

## What NOT to do

- Don't call `push_message` for normal request/response replies — return a decision
  envelope instead. The plugin handles the send for you.
- Don't call `push_message` to "ping" or "say hi" to inactive groups. The reactive guard
  will return 422 and your message will be discarded.
- Don't try to set user profiles / facts via this skill. M1 has no user profile support.
- Use `submit_control(reply_to="m:123")` to quote a message already visible in this chat's
  history; it applies to the very next body decision. It is resolved through the plugin
  mapping; unknown, legacy or cross-chat ids degrade to an ordinary unquoted message.
  Never pass a NapCat message id.
- Don't call `get_message_images` speculatively for every reactive turn — only when the
  user's text actually references a past image. Each call costs bytes + an extra LLM turn.
- Don't call `get_message_images` against `[语音] [视频] [文件:...]` placeholders inside a
  `<forwarded_messages>` block — those have no fetchable URL. Forward **images** there
  (numbered `[转发图#x]`) are fine to fetch by number — see "Forwarded message blocks".
