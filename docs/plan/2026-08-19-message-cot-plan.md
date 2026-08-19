# Feishu CoT progress message implementation plan

Source request: "check how this project handle COT:
https://github.com/wenext-limited/wenext-lark-bridge" and then "scope that" —
port that project's native Feishu CoT progress surface into feishu-botd.

## Facts and scope

- `wenext-lark-bridge` renders agent progress with a first-party Feishu API,
  `im/v1/message_cot`, not with cards. Its Feishu client sends only
  `msg_type: "text"`; the repository contains no CardKit, `collapsible_panel`,
  or `streaming_mode` usage at all.
- The API is three calls: `POST /open-apis/im/v1/message_cot` (create, bound to
  the user's `origin_message_id`, returns `cot_id` + `message_id`), `PUT
  /open-apis/im/v1/message_cot` (append events), and `POST
  /open-apis/im/v1/message_cot/complete/{cot_id}?message_id=…&reason=done|error`.
- The update payload is an AG-UI event stream. Each event is
  `{event_type, content: <JSON string>, timestamp: <ms>}`. The bridge uses
  `RUN_STARTED`, `STEP_STARTED`, `STEP_FINISHED`, and `RUN_FINISHED`; the Go SDK
  names `TOOL_CALL_START` as another valid type.
- **The API is undocumented.** The doc path embedded in the SDK v3.9.10 comment
  (`.../reference/im-v1/message_cot/cot-message-brief`) returns "文档不存在" in
  both locales, the docs-site search for `message_cot` returns nothing, and the
  SDK comment links to an internal ByteDance console host. Treat it as an
  allowlisted or unreleased capability with no stability guarantee.
- **No SDK client exists.** Go v3.9.7 (pinned) and v3.9.10, and Python
  `lark-oapi` 1.7.2, all ship the `MessageCot` *model* (`event_type`, `content`,
  `timestamp`) but no resource methods. Calls must be hand-rolled over
  `larkcore.Request`. Bumping the SDK gains nothing here and is out of scope.
- The daemon's current timeline contract is cumulative markdown snapshots
  (`timeline_markdown`, `timeline_title` in `command.proto`), while
  `message_cot` consumes typed step events carrying `stepId`/`stepName`.
  Neither is derivable from the other, so this is an additive contract change,
  not a rendering-backend swap.
- The existing `collapsible_panel` timeline stays. It is the fallback whenever
  the CoT permission is absent, and it is the only path for tenants where the
  API is not allowlisted. No existing provider behavior changes.
- CoT progress is strictly best effort. A CoT failure must never fail the
  provider's RPC, mirroring both the bridge's hook contract and the daemon's
  existing `logFeishuFailure` handling.
- Progress content is a fixed vocabulary of sanitized step labels. No user
  text, raw commands, tool output, logs, or credentials are sent, matching the
  bridge's stated rule.

## Implementation plan

1. **Slice 0: Feasibility spike and kill gate** — files: none merged; a scratch
   probe only. Confirm the 动态消息 permission scope is grantable to our app in
   开发者后台, then exercise create/append/complete against a scratch chat over
   `larkcore.Request`. Resolve two known ambiguities: the SDK types `timestamp`
   as a string while the bridge sends integer milliseconds, and the event
   vocabulary beyond the bridge's proven four types is unverified
   (`TOOL_CALL_START` exists, its `content` fields are unknown). Acceptance: a
   CoT message is created, advanced, and completed in a real chat, and the
   accepted `timestamp` type and usable `event_type` set are recorded. If the
   permission cannot be granted, the demand ends here. Specialist: Feishu Go
   integration developer.

2. **Slice 1: Add the CoT transport** — files: `internal/feishu/message_cot.go`
   and focused adapter tests. Introduce a `CoTMessages` interface
   (`Create`/`AppendEvents`/`Complete`) alongside `DynamicCards`, implemented
   with raw `larkcore.Request` calls and boundary validation in the style of
   `cardkit.go`. Keep every raw-HTTP detail in this one file so a future SDK
   resource is a single-file replacement. Treat an "already in terminal status"
   response to complete as success, since the message is already in the state
   the call wanted. Acceptance: fake-backed tests prove create -> append ->
   complete ordering, terminal-state idempotency, and redacted failures.
   Specialist: Feishu Go integration developer. Depends on slice 0.

3. **Slice 2: Extend the provider contract** — files:
   `proto/feishubotd/v1/command.proto`, generated files under
   `gen/feishubotd/v1/`, `internal/config/config.go`, and config tests. Add
   `AgentTimelineStep` (`step_id`, `label`, state) as a repeated field on the
   Start/Update/Finish requests using existing reserved ranges, leaving
   `timeline_markdown` and `timeline_title` untouched. Add an
   `AllowCoTProgress` per-provider capability flag following the existing
   `AllowFollowUpMessages` pattern. Acceptance: generated bindings compile,
   existing providers are wire-compatible and behaviorally unchanged, and the
   new flag defaults to off. Specialist: Go API/protobuf developer. Depends on
   slice 0 for the event vocabulary only.

4. **Slice 3: Wire the response state machine** — files:
   `internal/service/agent_cot.go`, `internal/service/agent.go`,
   `internal/grpcapi/command.go`, and service/gRPC tests. Give a response an
   optional CoT handle beside its card: Start creates the CoT bound to the
   triggering message and emits `RUN_STARTED`; Update diffs the submitted step
   list into `STEP_STARTED`/`STEP_FINISHED`; Finish emits `RUN_FINISHED` and
   completes with `done`/`error` mapped from `AgentResponseOutcome`. Every CoT
   call is best effort and logged, never surfaced as an RPC failure. Acceptance:
   step transitions, duplicate and out-of-order updates, terminal mapping, and
   CoT-failure isolation are behavior-tested. Specialist: Go concurrency/service
   developer. Depends on slices 1 and 2.

5. **Slice 4: Survive restarts** — files: `internal/service/agent_cot.go`, state
   persistence under `StateDir`, and tests. Journal pending `{cot_id,
   message_id, outcome}` records so a crash mid-run does not leak a CoT message
   that spins forever, and close leftovers on startup. This is deliberately
   narrower than the bridge's SQLite store plus background compensator: a
   bounded journal and a startup sweep, not a general outbox. Acceptance: a
   simulated restart closes a pending CoT message exactly once. Specialist: Go
   service developer. Depends on slice 3.

6. **Slice 5: Fallback and documentation** — files: `docs/agent.md`,
   `README.md`, configuration docs. Document the capability flag, the required
   permission, the step contract, and the automatic fallback to the
   `collapsible_panel` timeline when CoT is unavailable. State plainly that the
   underlying API is undocumented and may change without notice. Acceptance: a
   provider author can emit steps, and understands what happens when the
   permission is missing, without reading daemon internals. Specialist: Go docs
   and integration developer. Depends on slices 3 and 4.

7. **Slice 6: Verification and adversarial closure** — files: tests and only
   fixes required by failures. Run formatting, proto lint/staleness checks, all
   tests including race, vet, and build. Acceptance: every documented behavior
   has a test, all repository gates pass, and the worktree diff contains no
   unrelated changes. Specialist: Go reviewer and verification engineer. Depends
   on all prior slices.

## Open questions

- Does the agent-side provider have discrete step boundaries to emit, or does it
  only produce the accumulated markdown timeline sent today? Slice 2's contract
  change is only useful if the provider can emit steps. If it cannot, the
  existing `collapsible_panel` timeline is as good as this gets until the
  provider changes too, and this demand should stop after slice 0.
- Whether the CoT surface should eventually replace the panel for permitted
  tenants, or remain a parallel surface indefinitely. This plan assumes
  parallel; nothing here forecloses the other choice.

## Parallelism

Slice 0 gates everything and is serial. Slices 1 and 2 are independent and may
run in parallel once the event vocabulary is known. Slices 3, 4, and 5 are
serial after them. Verification is last. Documentation drafting may begin after
slice 2, but its final truth is gated on slices 3 and 4.
