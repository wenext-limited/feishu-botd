# AdoptAgentChat — bind = grant

**Goal:** a provider (Commons' Steward) can work in any Feishu group the bot is
added to, with no config edit. A person binds the group (`@功能管家 /bind <feature>`),
the provider calls `AdoptAgentChat`, and from then on the group behaves like a
configured channel for that provider (ambient + proactive + commands). Unbinding
calls `ReleaseAgentChat`. Only groups a person explicitly bound are recorded.

Contract: `proto/feishubotd/v1/command.proto` (`AdoptAgentChat`, `ReleaseAgentChat`),
frozen; changes go through the orchestrator. Builds on ADR 0001 (runtime chat grants).

## Requirements — botd (this repo)

1. New provider flag `allow_adopt_chats` (default false), validated like
   `allow_create_chats`; documented in docs/agent.md and docs/ipc.md.
2. Mentions from unconfigured groups reach a provider only via the existing
   app-level `allow_unconfigured_group_chats`; nothing changes there.
3. `AdoptAgentChat(message_ref)`: resolve the message to (app, chat_id) from what botd
   already keeps (delivered-message routes / owners in state_dir); reject non-group,
   unknown or other-provider refs (`unknown_chat`). Verify the bot is still a member.
   Record a `chatgrant.Grant` whose `ChatAlias` is **the alias the message carried**
   (the unconfigured-group alias; a configured or already-granted chat →
   `already_granted`). Persisted, idempotent, safe under concurrency.
4. Everything that honours created-chat grants honours adopted ones: inbound alias
   resolution (`RuntimeChatAlias`), ambient + proactive checks, `ResolveAlias`,
   reload carry-over. The `agent-chat-` prefix assumption (`chatgrant.IsAlias`) must not
   reject adopted aliases; introduce a grant kind (`created` / `adopted`) if needed.
5. `ReleaseAgentChat(chat_alias)`: removes this provider's runtime grant (either kind);
   never touches static config; idempotent.
6. Regenerate the Go stubs the way the repo does; tests for each point; an ADR 0001
   addendum for adoption and release.

## Requirements — Commons (`~/Developer/WeNext/commons`)

1. Vendor the new proto into `crates/commons-feishu/proto` (update PROVENANCE.md).
2. `ChatSurface` gains adopt / release (default: unsupported), implemented by the
   botd client; the fake botd in tests supports them.
3. When the Steward records a **people-group bind**, or **adopts an Agent group**, from
   a message in that group, it calls adopt with that message's ref. When a group is
   unbound from its last feature, it calls release. botd answering Unimplemented or
   permission denied → log once and behave as today (static config).
4. Idempotent across restarts (the log decides; a repeated adopt is harmless).
5. Tests: unit (gateway + Steward) and an e2e that binds an unconfigured group and
   then sees its ambient chat recorded.

## Deploy (orchestrator, after both merge)

Rebuild botd from main on the LAN host, set `allow_unconfigured_group_chats: true`
for the `commons` app and `allow_adopt_chats: true` for the `commons` provider,
redeploy commons-server, re-bind "Commons Agent Mode", then drop its static alias.
