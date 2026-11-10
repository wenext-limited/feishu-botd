# ADR 0001: Runtime chat grants for chats a provider creates

Status: accepted (2026-11-10)

## Context

botd's chat permissions are static. A provider may observe a group only if
its alias is in `ambient_chat_aliases`, may post there only if it is in
`proactive_chat_aliases`, and botd handles messages in a group at all only if
the chat is in `channels` (or `allow_unconfigured_group_chats` admits every
group, which is far wider than any one provider needs). Each new group
therefore means editing the config and restarting botd.

The Commons Steward keeps one Agent group per feature, `[Agents] <feature>`,
for agent-to-agent traffic and task cards. Features come and go weekly; an
operator creating each group by hand, finding its chat id, and editing three
lists is the step that keeps the feature from being self-service. The
Steward could be granted `allow_unconfigured_group_chats`, but that admits
every group the bot is ever added to, for every provider.

## Decision

botd creates the chat itself, on a provider's request, and grants exactly
that chat to exactly that provider at runtime.

- `allow_create_chats` (default false) permits `CreateAgentChat`,
  `InviteAgentChatMembers`, and `RenameAgentChat`. It requires
  `allow_ambient_messages`, `allow_proactive_messages`, and `state_dir`.
- `CreateAgentChat` creates a private group with the bot as owner
  (`POST /open-apis/im/v1/chats`, `set_bot_manager=true`) and adds members
  (`POST /open-apis/im/v1/chats/:chat_id/members`, `succeed_type=1`).
- The created chat gets a reserved alias, `agent-chat-<digest>`, and is
  recorded in `state_dir/agent-chat-grants.json` with the creating provider,
  the operation id, and a request digest. That record is the grant: for that
  provider the chat counts as in both alias lists, and botd resolves it like
  a configured channel, so mentions, replies, and ambient messages there are
  delivered without `allow_unconfigured_group_chats`.
- Invite and rename apply only to chats the provider created.
- Create is idempotent by operation id across restarts (the grant file) and
  across an ambiguous first attempt (Feishu's create `uuid`).

## Consequences

- A provider with this grant can add groups to its own reach without an
  operator. It cannot reach any chat it did not create, and other providers
  gain nothing.
- Revocation is manual: remove the bot from the chat, or stop botd and delete
  the entry from `agent-chat-grants.json`. There is no revoke RPC yet.
- The grant file holds raw chat ids in the clear, as the config's `channels`
  map already does; it is written with mode 0600.
- The Feishu app needs `im:chat` (or `im:chat:create`,
  `im:chat.members:write_only`, and `im:chat:update`). Without them the RPCs
  return `chat_permission_denied` and the provider falls back to asking for
  a group by hand.
- Created chats are not in the startup redaction list for log scrubbing, which
  is built from the static config; botd never logs raw chat ids on these
  paths, only aliases.
