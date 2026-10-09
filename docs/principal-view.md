# Principal view

Refs smarty-dev#5049. Principal view keeps the principal's conversation in the **existing Pi pane**; it does not create another feed or terminal. Voice, the editor, history, status, native extensions, and inbox widgets keep their normal owners and keybindings.

## Toggle and persistence

- `/principal-view` or **Ctrl+Alt+P** toggles on/off.
- `/principal-view on`, `/principal-view off`, `/principal-view auto` select explicitly.
- `/fabric settings` → **UI** → **Principal view** exposes the same persisted `ui.principalView` setting (`auto|on|off`).
- `auto` is on for exact `org` and `org-agent` roles, otherwise off. `PI_FABRIC_ROLE` wins over `SMARTY_ROLE`; the suffix after `@` is ignored. Fleet `bin/smarty-role` exports `SMARTY_ROLE=org-agent@SHA`.
- The command saves project config when trusted, global agent-dir config otherwise, following Fabric settings scope. It bootstraps configuration only, never starts optional runtime engines just to change the display.
- Existing `ui.incomingMessages` preferences remain compatible per persisted layer: collapsed→on, expanded→off, auto→auto. An explicit `principalView` in that layer wins. There is only one settings row.

On reduces each native incoming agent, actor or mail carrier (including a batched burst) to one dim `↳ sender: [count kind] preview` line, with about 80 body characters. Mixed senders are shown as `sender +N sender(s)`; mixed kinds are listed, rather than attributing the whole burst to its first sender. The kind comes from the delivery/event envelope (`steer`, `followUp`, etc.); `mail.inbound` notices are labeled explicitly. Delivered inbox shadows render nothing; unseen inbox work still appears. **Ctrl+O** temporarily reveals full incoming messages and tool output without changing the persisted preference. Off restores full native incoming rendering and the tool expansion state captured before entering principal view. A fresh session gets a fresh expansion snapshot.

Org-labeled incoming replies (`org` or `org-agent`, optionally `@revision`) and explicitly principal-addressed carriers (`to="principal"` in the envelope or `details.to`/`details.data.to`) retain their full native rendering. These labels affect display only, never authority or routing. An originating `principal` receipt is not a destination and does not exempt ordinary chatter. If a burst mixes a principal-facing reply with chatter, the whole carrier remains full so the reply cannot be clipped. If such an inbox burst also carried an already-delivered shadow, the carrier is rendered in the same full native style with only the remaining rows; the shadow stays hidden until expanded. Principal user messages and the local assistant's replies remain native/full. No adjacent delivery entries are merged or rescheduled.

`tests/incoming-messages.test.ts` includes a native `CustomMessageComponent` before/after snapshot: a two-sender follow-up burst becomes one line and expands back to the exact original rendering. Tests also assert byte-identical model context. This is renderer evidence, not Paul's live m5 Herdr acceptance screenshot.

This is a display projection only. No input/context/message replacement handlers are registered. User content, incoming carriers, assistant replies, model thinking level, active tools, and LLM-facing bytes are untouched. Thinking visibility stays native (Ctrl+T) because installed Pi has no public thinking-display setter.

## Feed parity and the installed Pi limitation

The reference is Paul's validated `feed-ref.mjs`, which uses Pi TUI components. Its principal block is **plain Text**, not Markdown: a bold yellow `YOU · <time>` label, paddingX=1/paddingY=0, background `\x1b[48;5;237m…\x1b[49m`, and one unshaded blank line above and below. Assistant text is Markdown.

The newest installed runtime on the lane is `623f57905b902feafdaeb6b5aeed9812fafcf453`. Its complete `docs/extensions.md`, `docs/tui.md`, declarations and native components were inspected:

- `registerMessageRenderer` accepts **CustomMessage** only, not native user messages.
- `registerMarkdownTransformer` accepts/returns Markdown strings with `messageType`/`isStreaming`/`availableWidth`; it cannot own the user component's background, label container or outside spacing.
- `UserMessageComponent` hardcodes `Box(outputPad, 1, theme.userMessageBg)` around Markdown. Its OSC 133 zones are native.
- `ctx.ui.getToolsExpanded/setToolsExpanded` are public and supported in TUI.
- There is no user-component renderer or thinking-display get/set pair. `setHiddenThinkingLabel` only changes a label; `setThinkingLevel` changes model behavior, so neither is a substitute.

**Therefore exact YOU styling is not implemented in this extension release.** No prompt rewrites, ANSI injection through Markdown, global theme replacement, prototype mutation, duplicate transcript or second TUI are used. Full feed parity needs the small fork hook below and Paul's validation in the real org pane.

| Feed feature | Principal view on installed Pi | Native Pi |
| --- | --- | --- |
| Incoming peer/actor/mail chatter | One dim line; Ctrl+O reveals full text | Full custom-message block |
| Delivered inbox shadow | Hidden; visible when expanded/off | Full duplicate block |
| Principal YOU label, bg 237, outer blank lines | **Gated on Pi-fork hook**; native user block retained | Theme-shaded Markdown; no YOU label |
| Assistant Markdown/replies | Full and unchanged | Full |
| Tool output | Collapsed with public API; Ctrl+O expands | Ctrl+O |
| Thinking display | Unchanged; use Ctrl+T | Ctrl+T |
| Animated state, voice, inbox widget | Existing Pi/extensions retained, not reimplemented | Native loader/status + installed extensions |
| Editor/multiline keys/history | Native and unchanged | Native |

## Smallest proposed change to Smarty-Pants-Inc/pi (not applied here)

Add a **per-UI display-only setter**, analogous to `setEditorComponent`, rather than teaching Fabric to patch transcript internals:

```ts host
ctx.ui.setUserMessageRenderer(
  renderer?: (text: string, options: { outputPad: number; timestamp: number }, theme: Theme)
    => Component | undefined,
): void;
```

Three host source seams suffice:

1. `packages/coding-agent/src/core/extensions/types.ts`: declare/export the callback and setter; RPC/print UI implementations return a no-op as for other terminal-only setters.
2. `packages/coding-agent/src/modes/interactive/components/user-message.ts`: accept the original text/timestamp and optional callback; `undefined` result uses the **exact existing native block**. Keep OSC 133 markers outside either rendering. The callback sees text read-only; it never touches session messages, context, image/skill attachments, or provider input.
3. `packages/coding-agent/src/modes/interactive/interactive-mode.ts`: own the callback per extension UI lifecycle; pass it to live and history/replayed user components. Setter updates/rebuilds only existing user components and requests a render, preserving editor, scroll anchor, assistant and live tool state. Clear it on extension reload/session teardown.

Fabric would then set a callback only while principal view is on. It returns `Container(Spacer(1), Text(bold yellow YOU · timestamp + newline + original text, 1, 0, bg237), Spacer(1))`; off clears it. Use the host-supplied component interfaces, not a second host package copy.

Fork checks: byte-identical session/provider context; exact SGR 48;5;237 and reset; bold yellow label; exactly one outside blank line above/below; Unicode/narrow-width clipping; multiline/plain text parity; attachments/skills preserved; live/history/reload and both themes; native fallback exact; switching modes preserves editor, scroll and live tools. Render on/off in the actual org pane before claiming issue acceptance.
