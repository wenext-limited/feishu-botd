# Feishu agent support implementation plan

Source request: "Please improve feishu-botd, such that we could have fully
support on it. I want to build an agent in feishu with feishu-botd."

## Facts and scope

- The existing protobuf-first `CommandService` remains the inbound provider
  boundary. Existing `Subscribe` and final-only `Respond` behavior must remain
  backward compatible.
- `SubscribeAgentEvents(include_unmatched_messages = true)` is the natural
  agent path: it receives otherwise-unhandled group mentions and direct
  messages without changing the legacy command stream.
- Agent output uses Feishu CardKit card entities (`schema: 2.0`) with native
  streaming text updates, not repeated message creation or the legacy message
  patch helper.
- Feishu credentials, raw chat IDs, card IDs, and outbound message IDs remain
  private to botd. Providers use `delivery_id` and an opaque `response_id`.
- Card mutations are serialized by botd. Provider revisions are contiguous and
  idempotent; botd owns Feishu's card-wide sequence counter.
- Process-local session state is sufficient for this slice. A restart loses
  active response handles but cannot disable streaming on remote Feishu cards;
  durable outbox/recovery remains separate infrastructure work and is not
  silently implied.
- Card actions are delivered asynchronously to the provider that owns the
  response. Callback handling acknowledges Feishu immediately and never waits
  for provider work.

## Implementation plan

1. **Slice 1: Freeze the agent protocol** — files:
   `proto/feishubotd/v1/command.proto`, generated files under
   `gen/feishubotd/v1/`, and focused protobuf/gRPC tests. Add backward-compatible
   agent-event subscription plus start/update/finish response RPCs, action
   messages, response
   statuses, revisions, and conversation identity. Acceptance: generated
   bindings compile and old clients retain their existing RPCs. Specialist:
   Go API/protobuf developer.

2. **Slice 2: Add the native CardKit transport** — files:
   `internal/feishu/sender.go`, a focused CardKit adapter file, and Feishu
   adapter tests. Implement card entity creation, card-reference send/reply,
   cumulative element updates, and streaming finalization with UUID and sequence
   validation. Acceptance: fake-backed tests prove the exact create -> send ->
   update -> finish operation order and redacted failures. Specialist: Feishu Go
   integration developer. Depends on slice 1 only for shared naming, not code.

3. **Slice 3: Implement the response state machine** — files:
   `internal/service/command.go`, a dedicated agent-response state module, service
   tests, `internal/grpcapi/command.go`, and gRPC tests. Add provider ownership,
   opaque response handles, contiguous revision/idempotency rules, terminal
   states, per-response serialization, TTL cleanup, and compatibility with
   final-only `Respond`. Acceptance: start, multiple updates, duplicate retries,
   stale/gapped revisions, finish/fail/cancel, concurrent calls, and post-terminal
   rejection are behavior-tested. Specialist: Go concurrency/service developer.
   Depends on slices 1 and 2.

4. **Slice 4: Complete inbound agent routing and card actions** — files:
   `internal/feishu/receiver.go`, receiver tests, command broker routing, main
   wiring, and gRPC action-stream tests. Add exact-command-first/wildcard fallback,
   direct-message ingestion with private raw-chat routing, opaque conversation
   IDs, `card.action.trigger` normalization, and response-owner action delivery.
   Acceptance: a natural group mention or P2P prompt reaches one wildcard agent,
   streams a response to the correct chat, and a card action returns only to the
   owning provider. Specialist: Feishu event/concurrency developer. Depends on
   slice 3.

5. **Slice 5: Document and demonstrate the provider contract** — files:
   `README.md`, `docs/ipc.md`, configuration/deployment docs, and an example Go
   agent client under `examples/`. Document Feishu capabilities/scopes, CardKit
   schema requirements, response lifecycle, wildcard routing, action semantics,
   failure/restart behavior, and a runnable echo/stream example. Acceptance: a
   reader can generate bindings, subscribe, stream cumulative output, finish,
   and handle actions without reading daemon internals. Specialist: Go docs and
   integration developer. Depends on slices 1-4.

6. **Slice 6: Verification and adversarial closure** — files: tests and only
   fixes required by failures. Run formatting, proto lint/staleness checks, all
   tests including race, vet, build, and targeted concurrency/error-path tests.
   Acceptance: every documented behavior has a test, all repository gates pass,
   and the worktree diff contains no unrelated changes. Specialist: Go reviewer
   and verification engineer. Depends on all prior slices.

## Parallelism

This is one demand with tightly coupled protocol and state invariants. Slices
1-4 are serial. Documentation/examples may begin after slice 1, but their final
truth is gated on slices 2-4. Verification is last. Read-only API research and
test-design review may run in parallel; implementation edits will not overlap.
