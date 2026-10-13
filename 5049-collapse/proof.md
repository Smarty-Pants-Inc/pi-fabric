# Org-pane collapse proof

## Pins and commands

Worktree: `/home/paul/lanes/fv2-5049-collapse`.

- Before: local `origin/main` / lane start `c9b9a1d44cb723589bcbd54d46ed7fc7814c5993`; unchanged bundle saved in `.local/out/before-dist/` before editing production source.
- After: `2166778f87619a89fa59892ee30a5ad451522469`; fresh `dist/` bundle built from exactly those source bytes before the commit.
- Pi: repository's installed `@earendil-works/pi-coding-agent` 1.1.0, real CLI in a tmux PTY, not a custom transcript renderer.

```sh
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 -- nice -n 19 node .local/out/proof-driver.mjs before
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 -- nice -n 19 node .local/out/proof-driver.mjs after
nice -n 19 node .local/out/rasterize-captures.mjs
```

Observed output (the first two are preserved in `proof-before.log` / `proof-after.log`):

```text
PASS: real Pi TUI (before), SMARTY_ROLE=org-agent; separate real Pi peer sends 3 mesh-routed Fabric followUps + 1 native inbox shadow; full user/assistant and provider context.
PASS: real Pi TUI (after), SMARTY_ROLE=org-agent; separate real Pi peer sends 3 mesh-routed Fabric followUps + 1 native inbox shadow; full user/assistant and provider context; Ctrl+O expand/recollapse; byte-identical branch.
before-collapsed.png: 1344x1424; native dim summaries 0
after-collapsed.png: 1344x1424; native dim summaries 2
after-expanded.png: 1344x1424; native dim summaries 0
after-recollapsed.png: 1344x1424; native dim summaries 2
contact-sheet.png: before (left), after (right), no retouched text or colors
```

## Native path

The org TUI uses `SMARTY_ROLE=org-agent`, `ui.principalView=auto`, and no ambient fleet variables. The driver whitelists the launch environment and uses a private profile, mesh, run/session directories and localhost model endpoint. No host credentials, real fleet mesh or actor registry are accessed.

An ordinary principal input activates production Fabric through `fabric_exec`. A separate native Pi RPC peer (role `project-agent`, not a Fabric child task) calls three real `agents.followUp` operations against the org's exact session id. The real mesh control route delivers their bodies and attribution to production `MainAgentController`, which produces one native three-message burst. The third message is a `mail.inbound` notice. All three carry the recipient display marker `data.to="principal"`.

The proof extension sends one normal `rootInboxMessage` shadow carrier through native `pi.sendMessage`. It is org-labeled, distinct unseen work; it does not fake an authoritative delivery receipt. Its production builder and all renderers are unchanged. This exercises the inbox **rendering** path, not the root-inbox scheduler. Actor rendering and already-delivered-shadow dedup are covered by the targeted tests.

The provider holds the org's initial empty response until the peer's actual native settle, so the three follow-ups form one real burst. No sleeps, polling loops, context/message replacement, synthetic assistant transcript entries or mocked delivery are used. The org's one visible assistant reply comes from the normal streaming model path. The endpoint is offline test input, not an external model or principal-device acceptance.

Screens are captured with `tmux capture-pane -p` and `-p -e`. The driver sends actual `C-o` keys to the owned PTY. It asserts expanded native detail and recollapse, then compares the full persisted branch **byte-for-byte through both toggles**. It also verifies the full incoming bodies reach the org's provider requests and confirms the peer's sender id differs from the receiver's id.

## Evidence index

Successful before root: `.local/out/probe-before-8NHenY/`.

Successful after root: `.local/out/probe-after-h4NpkU/`.

Each root contains:

- `launch-command.txt`, `sender-command.txt`: exact isolated receiver and peer commands.
- `ready.json`, `activation.json`, `settled.json`: native TUI mode, org-role signal and session pins.
- `sender-stdout.jsonl`, `delivery-outcome.json`: real peer `fabric_exec`, mesh-routed followUp acknowledgements, and native settle.
- `branch.json`, `provider-requests.json`: complete received carriers and actual provider context.
- `shutdown.json`: receiver emitted native `session_shutdown`.
- `sender-exit.json`: actual peer process closed with exit code 143 after its owned stop; the driver awaited closure.

After root also contains `after-collapsed.json`, `after-expanded.json`, `after-recollapsed.json`, with native expansion state and identical branches. A separate direct check (`nice -n 19 node .local/out/capture-integrity.mjs`) compared the serialized native branch bytes from all three snapshots: PASS, SHA256 `d045d47dad5a35d9b3e7d812724815d3b33ce9973aa9d1918fddbdeea3458bdf`. `.local/out/capture-integrity.json` pins that branch digest and all 13 capture artifacts. Final native `tmux list-sessions` checks found no server running on either successful probe socket.

Captures (all relative to `.local/out/`):

- `before-collapsed.txt`, `before-collapsed.ansi.txt`, `before-collapsed.png`.
- `after-collapsed.txt`, `after-collapsed.ansi.txt`, `after-collapsed.png`.
- `after-expanded.txt`, `after-expanded.ansi.txt`, `after-expanded.png`.
- `after-recollapsed.txt`, `after-recollapsed.ansi.txt`, `after-recollapsed.png`.
- `contact-sheet.png`: before left / after right, direct combination of those PNGs.

PNG rasterization uses local proof-only `@napi-rs/canvas` with DejaVu Mono. It retains captured SGR colors and text, uses a black terminal-default background, and checks the summary color is the actual native dim theme value (RGB 126,136,142). No graphical redesign, generated image or retouched UI is involved.

## Bounds and remaining owner gates

No push, GitHub, SSH, credential-store or child-task calls. The incoming-message suite has no destructive cleanup paths; no destructive suite was run. Builds and proof processes use the assigned memory/swap caps and low priority. Every owned PTY/RPC probe was closed in its driver `finally` block; latest native close receipts are above. Evidence and scratch paths stay inside this worktree / the assigned TMPDIR.

The build's native helpers are explicitly **UNPINNED local artifacts**, not release candidates. No install, independent acceptance audit, current-remote-main rebase or pre-PR publication gate is claimed here. Those remain with the lead under the assignment's no-publication/no-child constraints. The outcome proven here is the isolated real-Pi user-visible path.
