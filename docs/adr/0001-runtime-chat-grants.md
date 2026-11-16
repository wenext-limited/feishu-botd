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

## Addendum: adoption and release (2026-11-16)

A provider may now explicitly adopt a group a person has bound, without editing
static config. `allow_adopt_chats` is a separate, default-off capability for
`AdoptAgentChat` and `ReleaseAgentChat`. Like creation it requires ambient and
proactive capabilities and `state_dir`, and permits empty static alias lists.
The initial mention from an unconfigured group still requires the app-level
`allow_unconfigured_group_chats` flag.

Adoption names the group by any unexpired message delivered to that provider
from it: a command, an owned-message reply, or ambient observation, including the
latest message rather than the original bind. The sealed route journal supplies
the private app/chat ids and the exact delivered alias. Only group ingress
retains unauthored message routes; outbound routes are marked `Authored`, so
these facts distinguish eligible group messages without changing the journal
format. The ownership snapshot alone cannot resolve an inbound message or prove
its receipt. Routes are now retained for adoption-enabled recipients even before
a chat has a proactive grant. Dispatch persists actual recipients while holding
the ingress lock; route lookups share that lock so an immediate client call
waits for publication. A failed write cannot leave a prospective recipient with
adoption access. The existing route TTL and app-secret rotation behavior apply.

botd checks provider/app scope and current bot membership through Feishu's
`is_in_chat` API before granting. Unknown, expired, foreign, bot-authored and
non-group refs fail as `unknown_chat`. An alias remapped since delivery also
fails closed. Static chats and this provider's existing grants are idempotent
`already_granted` no-ops; static provider ACLs are never widened by adoption.
The alias remains exactly the one the message carried, including an opaque
unconfigured-group alias. The resulting grant resolves without a message route
or app-wide unconfigured ingress, and survives restart and reload.

The grant file remains version 1. Each grant has `kind: "created"` or
`kind: "adopted"`; absent kind loads as created for backward compatibility.
Created aliases retain the reserved `agent-chat-` prefix. Adopted aliases are
not required to have that prefix. Registry entries are scoped by provider and
alias, so independent providers can adopt the same chat; shared aliases must
resolve to the same app/chat pair. Ambient and proactive access checks the
capability corresponding to each grant kind. Rename and invite require a
created grant and `allow_create_chats`, regardless of an adopted alias's prefix.

`ReleaseAgentChat` atomically removes this provider's runtime grant of either
kind. It is idempotent, never edits static config, and does not delete the chat
or remove its members. Other providers' grants and independent static permissions
survive. Without any remaining runtime or static route, mentions again depend
on the unconfigured-group flag; the released provider loses runtime ambient and
proactive access. Removing a created grant also removes its operation record
and management rights: later rename/invite calls return `unknown_chat`, and
re-adopting creates only an adopted grant. A later create operation is again
subject to Feishu's UUID window. This supersedes the manual-only revocation
limitation above. Creation, adoption, and release are serialized within the
service; registry writes and removals persist before changing memory.
