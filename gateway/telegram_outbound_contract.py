"""Telegram outbound mutation contract and call-site inventory.

This module is the design contract for issue #2.  It is intentionally not imported by
the Telegram adapter and does not throttle, queue, retry, persist, or otherwise change
runtime behavior.  A later limiter can consume this contract without rediscovering the
semantics from individual ``Bot`` calls.

The implementation lives in
``plugins/platforms/telegram/adapter.py``.  The old
``gateway/platforms/telegram.py`` path is not present on this branch.

Contract vocabulary
-------------------

``durability`` answers what queue pressure may discard:

* ``durable`` means the mutation is part of the user-visible result or a required
  control/configuration operation.  It is never coalesced or silently dropped.
* ``replaceable`` means only the newest value for a stable lane is useful.  The
  coalescing key must include the profile/bot identity, chat, topic, and operation
  lane (for example, a stream or status key), but never message content.
* ``best_effort`` means the mutation improves the UI but is not the result.  It may
  be dropped under pressure and must not block a durable send.

``priority`` is ordered from lowest numeric value to highest:
``urgent`` (0), ``high`` (10), ``normal`` (20), ``low`` (30).  Priority is a
scheduling hint, not permission to overtake an already-dispatched mutation for the
same chat.  In particular, a final message cannot overtake an earlier chunk.

``retry_ambiguity`` describes the result of a failed attempt:

* ``safe`` means the request is known not to have been sent or accepted.
* ``conditional`` means only the operation-specific fallback is safe, such as a
  definitive parse/capability rejection, a deleted thread anchor, or a bounded
  flood retry.
* ``ambiguous`` means Telegram may have accepted the request.  Do not replay the
  same user-visible payload merely because confirmation timed out.
* ``none`` means this best-effort mutation has no replay policy.

``TelegramOutboundMetadata`` is the future queue item contract.  ``coalescing_key``
is an actual key supplied by the caller, not a payload hash.  Durable items must
leave it unset; replaceable items must provide it.  The current metadata dictionaries
used for routing remain unchanged because this module is not runtime wiring.

Routing and ordering invariants
-------------------------------

* A route is scoped by the receiving bot/profile, normalized ``chat_id``, and the
  Telegram topic/reply metadata.  Forum topics use ``message_thread_id``.  Private
  DM topics may need ``message_thread_id`` plus
  ``telegram_reply_to_message_id``; anchorless synthetic sends may use
  ``direct_messages_topic_id``.  Channel DM topics use
  ``direct_messages_topic_id`` rather than a bare thread id.
* ``cron.scheduler_delivery._live_route_metadata`` probes an ambiguous live
  Telegram target with ``get_chat_info`` before choosing forum-topic versus
  channel-DM-topic routing.  The standalone sender receives the already selected
  ``thread_id`` and maps it to ``message_thread_id``; it has no live adapter
  state or shared limiter.
* ``TelegramAdapter`` already serializes text and media API calls with a
  re-entrant per-chat lock.  The future queue must preserve that ordering.  A
  long text response is several ``sendMessage`` mutations: chunks are sent in
  order, continuation chunks reply-chain to the preceding message, and the
  delivered message IDs are retained.  A partial result must resume only the
  certain-undelivered tail, never resend a landed head.
* A missing thread or deleted reply anchor has an operation-specific route
  fallback.  Forum sends may drop a stale thread after the bounded retry and prune
  its binding.  Private DM-topic sends drop the topic and reply anchor together
  when Telegram rejects that route.  This is not permission for a generic retry.
* Rich sends have a deliberate distinction between a definitive rejection and an
  unknown/transient failure.  A definitive capability/format rejection may fall
  back to legacy MarkdownV2.  A transient or unknown ``sendRichMessage`` failure
  returns a failure without a legacy resend, because the rich request may have
  landed.

Budget semantics (future limiter only)
--------------------------------------

There is no limiter in this change.  When one is added:

* The global bucket is per Telegram bot credential and counts every Bot API
  mutation attempt, including retries and fallback attempts.  Separate credentials
  are separate global buckets.  Read-only probes such as ``get_chat`` and
  ``get_me`` are not message mutations.
* The per-chat bucket is keyed by the same normalized bot/profile/chat/topic
  route used by the queue.  One logical response can consume multiple units:
  text chunks, media bubbles, album requests, edits, and operation-specific
  retries are separate API attempts.  ``sendMediaGroup`` is one request for the
  default accounting unit; the catalog does not silently multiply it by photo
  count.
* A limiter must not charge both a logical adapter call and each underlying Bot
  API call.  Charge at the Bot API mutation boundary, or define a single explicit
  logical-to-physical expansion, but never both.
* Per-chat serialization remains necessary even with a global bucket.  A global
  token becoming available must not allow two concurrent chunk sequences for one
  chat to interleave.
* Existing short flood waits are bounded inline.  Long Telegram ``retry_after``
  values return a typed flood failure and arm the adapter's per-chat cooldown
  instead of sleeping on the gateway loop.  The future limiter should honor that
  cooldown and must not probe the chat by sending more mutations.

Queue pressure, shutdown, and restart
-------------------------------------

* Durable final text, final media, control prompts, and required topic/configuration
  mutations are admitted ahead of replaceable and best-effort work.  They are not
  evicted to make room for drafts, typing, reactions, preview deletion, or picker
  repainting.  If a durable item cannot be admitted, the owner must receive an
  explicit failure; silent loss is not a valid queue outcome.
* Replaceable work keeps the newest item for its key and discards stale pending
  values before dispatch.  Best-effort work may be dropped when the queue is full.
  Neither policy may discard a durable item or a chunk already acknowledged by
  Telegram.
* Shutdown must stop admitting new low-priority UI work first, preserve per-chat
  order for mutations already dispatched, and report durable items that were never
  dispatched so their owner can apply its existing recovery path.  It must not
  invent a retry for an in-flight ambiguous request.
* Queue contents, message bodies, coalescing state, and in-memory cooldowns are
  process-local.  No message payload is persisted across restarts.  A restart
  therefore cannot replay an arbitrary queued mutation from metadata alone.
  Cron's durable delivery handoff/execution ledger remains the source of restart
  recovery for cron work; an uncertain claimed send is not blindly replayed.
* The standalone ``standalone_sender_fn`` path constructs a fresh PTB ``Bot``.
  It does not share the live adapter's per-chat lock, cooldown map, or future
  queue.  Its text sender retries bounded transient 5xx/429 responses but never
  retries timeouts; media has only its explicit thread/parse fallbacks.  A
  standalone timeout remains ambiguous.
* Cron live delivery distinguishes a timeout before coroutine dispatch from one
  after dispatch.  The former may fall through to standalone.  The latter is
  treated as possibly delivered and suppresses standalone fallback to avoid a
  duplicate.  Relay-owned targets have no standalone fallback.

Direct Bot API mutation inventory
---------------------------------

The catalog below is exhaustive for outbound Telegram mutations in the current
adapter.  ``getUpdates``, ``getMe``, ``getChat``, media download calls, and other
read-only probes are deliberately outside this inventory, although ``getChat`` is
called by cron to resolve ambiguous topic routing.

The indirect gateway seams are part of the inventory because a limiter attached only
to ``TelegramAdapter.send`` would miss native media, control prompts, callback
answers, lifecycle calls, and the standalone process:

* text/final: ``TelegramAdapter.send`` →
  ``_send_text_locked``/``_send_chunks``;
  ``tools.send_message_senders._send_telegram``;
  ``tools.send_message_tool._send_to_platform``;
  ``gateway.stream_consumer_transport`` final/fallback sends;
  ``cron.scheduler_delivery._live_send_text`` and
  ``_standalone_send``;
* media: ``TelegramAdapter.send_voice``, ``send_image_file``, ``send_image``,
  ``send_document``, ``send_video``, ``send_animation``,
  ``send_multiple_images``; gateway media callers in
  ``gateway/run_turn.py``, ``gateway/run_topics.py``,
  ``gateway/run_notifications.py``, ``gateway/run_voice.py``,
  ``gateway/slash_commands_session.py``, and
  ``gateway/kanban_watchers.py``; standalone media in
  ``tools.send_message_senders._telegram_send_one_media``;
* control/configuration: ``send_update_prompt``, ``send_exec_approval``,
  ``send_slash_confirm``, ``send_clarify``, ``send_model_picker``,
  ``send_choice_picker``, ``_create_dm_topic``, ``rename_dm_topic``,
  ``_register_command_menu``, ``_ensure_forum_commands``,
  ``_set_status_indicator``, and ``_delete_webhook_best_effort``;
* UI callbacks: ``_handle_*_callback``, ``_picker_edit``,
  ``_edit_result_text``, ``_edit_html_quiet``, ``_edit_md_quiet``,
  ``_handle_inline_query``, and ``_set_reaction``.

The invariant tests named in the catalog cover the existing behavior.  They are
behavior tests, not source-shape tests; a future implementation may move a call
site as long as the same contract still holds.
"""

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Final


class OutboundPriority(IntEnum):
    """Scheduling order for admitted mutations; lower values run first."""

    URGENT = 0
    HIGH = 10
    NORMAL = 20
    LOW = 30


class OutboundDurability(str, Enum):
    """Whether a pending mutation may be replaced or discarded."""

    DURABLE = "durable"
    REPLACEABLE = "replaceable"
    BEST_EFFORT = "best_effort"


class RetryAmbiguity(str, Enum):
    """Whether replaying a failed API attempt can duplicate an accepted mutation."""

    SAFE = "safe"
    CONDITIONAL = "conditional"
    AMBIGUOUS = "ambiguous"
    NONE = "none"


class TelegramMutation(str, Enum):
    """Named mutation lanes in the direct-call inventory."""

    FINAL_TEXT = "final_text"
    FINAL_MEDIA = "final_media"
    OVERFLOW_CONTINUATION = "overflow_continuation"
    STREAM_DRAFT = "stream_draft"
    PROGRESSIVE_EDIT = "progressive_edit"
    FINAL_EDIT = "final_edit"
    COMMENTARY = "interim_commentary"
    CONTROL_PROMPT = "control_prompt"
    STATUS_UPDATE = "status_update"
    PREVIEW_DELETE = "preview_delete"
    TYPING = "typing"
    REACTION = "reaction"
    TOPIC_CREATE = "topic_create"
    TOPIC_RENAME = "topic_rename"
    TOPIC_SEED = "topic_seed"
    COMMAND_MENU = "command_menu"
    STATUS_INDICATOR = "status_indicator"
    WEBHOOK_CLEANUP = "webhook_cleanup"
    CALLBACK_ACK = "callback_ack"
    CALLBACK_EDIT = "callback_edit"
    INLINE_ANSWER = "inline_answer"


@dataclass(frozen=True, slots=True)
class TelegramOutboundMetadata:
    """Typed metadata a future Telegram outbound queue must receive.

    ``operation`` identifies the catalog row.  ``coalescing_key`` is scoped
    identity plus destination plus lane, never message content.
    """

    operation: TelegramMutation
    priority: OutboundPriority
    coalescing_key: str | None
    durability: OutboundDurability
    retry_ambiguity: RetryAmbiguity

    def __post_init__(self) -> None:
        if self.coalescing_key == "":
            raise ValueError("coalescing_key must be None or a non-empty string")
        if (
            self.durability is OutboundDurability.DURABLE
            and self.coalescing_key is not None
        ):
            raise ValueError("durable Telegram mutations cannot be coalesced")
        if (
            self.durability is OutboundDurability.REPLACEABLE
            and not self.coalescing_key
        ):
            raise ValueError("replaceable Telegram mutations require a coalescing key")


@dataclass(frozen=True, slots=True)
class TelegramMutationSpec:
    """One row in the direct-call/classification inventory."""

    operation: TelegramMutation
    api_methods: tuple[str, ...]
    call_sites: tuple[str, ...]
    durability: OutboundDurability
    priority: OutboundPriority
    coalescing_scope: str | None
    retry_ambiguity: RetryAmbiguity
    routing: str
    budget: str
    invariant_tests: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.durability is OutboundDurability.DURABLE
            and self.coalescing_scope is not None
        ):
            raise ValueError("durable inventory rows cannot declare coalescing")
        if (
            self.durability is OutboundDurability.REPLACEABLE
            and not self.coalescing_scope
        ):
            raise ValueError("replaceable inventory rows require a coalescing scope")

    def metadata(
        self, *, coalescing_key: str | None = None
    ) -> TelegramOutboundMetadata:
        """Materialize queue metadata after a caller resolves the route key."""
        return TelegramOutboundMetadata(
            operation=self.operation,
            priority=self.priority,
            coalescing_key=coalescing_key,
            durability=self.durability,
            retry_ambiguity=self.retry_ambiguity,
        )


TELEGRAM_MUTATION_CATALOG: Final[tuple[TelegramMutationSpec, ...]] = (
    TelegramMutationSpec(
        TelegramMutation.FINAL_TEXT,
        ("sendRichMessage", "sendMessage"),
        (
            "plugins.platforms.telegram.adapter:TelegramAdapter.send",
            "TelegramAdapter._try_send_rich",
            "TelegramAdapter._send_chunk_markdown_or_plain",
            "tools.send_message_senders:_send_telegram",
            "gateway.stream_consumer_fallback:_send_with_flood_retry",
            "cron.scheduler_delivery:_live_send_text",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.HIGH,
        None,
        RetryAmbiguity.CONDITIONAL,
        "normalized bot/profile/chat + topic metadata + optional reply anchor; per-chat FIFO",
        "one unit per Bot API attempt; formatted text may expand to ordered chunks",
        (
            "tests/gateway/test_telegram_final_delivery.py",
            "tests/gateway/test_telegram_rich_messages.py",
            "tests/gateway/test_telegram_thread_fallback.py",
            "tests/gateway/test_telegram_split_send_flood.py",
            "tests/tools/test_send_message_tool.py",
            "tests/cron/test_cron_live_delivery_confirmation.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.FINAL_MEDIA,
        (
            "sendPhoto",
            "sendDocument",
            "sendAudio",
            "sendVideo",
            "sendVoice",
            "sendAnimation",
            "sendMediaGroup",
        ),
        (
            "TelegramAdapter._send_media",
            "TelegramAdapter.send_voice",
            "TelegramAdapter.send_image_file",
            "TelegramAdapter.send_image",
            "TelegramAdapter.send_document",
            "TelegramAdapter.send_video",
            "TelegramAdapter.send_animation",
            "TelegramAdapter.send_multiple_images",
            "tools.send_message_senders:_telegram_send_one_media",
            "cron.scheduler_delivery:_live_send_media",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.NORMAL,
        None,
        RetryAmbiguity.CONDITIONAL,
        "same chat/topic/reply route as the associated text; per-chat FIFO; album chunks of ten",
        "one unit per media API request; a media group is one default unit",
        (
            "tests/gateway/test_telegram_documents.py",
            "tests/gateway/test_telegram_audio_vs_voice.py",
            "tests/gateway/test_telegram_voice_caption_markdown.py",
            "tests/gateway/test_telegram_voice_v0_regressions.py",
            "tests/gateway/test_send_multiple_images.py",
            "tests/cron/test_media_delivery_parity.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.OVERFLOW_CONTINUATION,
        ("sendMessage",),
        (
            "TelegramAdapter._send_overflow_continuation",
            "TelegramAdapter._edit_overflow_split",
            "TelegramAdapter._send_chunks",
            "gateway.stream_consumer_fallback:_send_new_chunk",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.HIGH,
        None,
        RetryAmbiguity.CONDITIONAL,
        "ordered reply chain; each continuation targets the prior landed message",
        "one unit per continuation; resume only certain-undelivered chunks",
        (
            "tests/gateway/test_telegram_overflow_partial.py",
            "tests/gateway/test_telegram_split_send_flood.py",
            "tests/gateway/test_stream_consumer_fresh_final.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.STREAM_DRAFT,
        ("sendRichMessageDraft", "sendMessageDraft"),
        (
            "TelegramAdapter.send_draft",
            "TelegramAdapter._try_send_rich_draft",
            "gateway.stream_consumer_transport:_send_draft_frame",
        ),
        OutboundDurability.REPLACEABLE,
        OutboundPriority.LOW,
        "bot/profile/chat/topic/turn/draft_id",
        RetryAmbiguity.NONE,
        "chat + topic metadata; ephemeral draft has no message id",
        "one unit per draft frame; stale frames may be coalesced before dispatch",
        (
            "tests/gateway/test_stream_consumer_draft.py",
            "tests/gateway/test_telegram_send_draft_format.py",
            "tests/gateway/test_telegram_rich_messages.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.PROGRESSIVE_EDIT,
        ("editMessageText",),
        (
            "TelegramAdapter.edit_message(finalize=False)",
            "TelegramAdapter._edit_text",
            "gateway.stream_consumer_transport:_edit_message",
            "gateway.run_turn:_run_agent_edit_streamed_message",
        ),
        OutboundDurability.REPLACEABLE,
        OutboundPriority.LOW,
        "bot/profile/chat/topic/stream-or-preview",
        RetryAmbiguity.CONDITIONAL,
        "existing message id plus the stream/status route; never create a new head for an edit",
        "one unit per edit attempt; saturated previews should not be resent",
        (
            "tests/gateway/test_telegram_progress_edit_transient.py",
            "tests/gateway/test_stream_consumer.py",
            "tests/gateway/test_stream_final_contract.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.FINAL_EDIT,
        ("editMessageText",),
        (
            "TelegramAdapter.edit_message(finalize=True)",
            "TelegramAdapter._try_edit_rich",
            "gateway.stream_consumer_transport:_edit_message",
            "gateway.run_turn:_run_agent_edit_streamed_message",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.HIGH,
        None,
        RetryAmbiguity.CONDITIONAL,
        "existing preview/message id plus the original chat/topic route",
        "one unit per final edit attempt; a rich capability rejection may use the legacy edit",
        (
            "tests/gateway/test_stream_final_contract.py",
            "tests/gateway/test_stream_consumer_fresh_final.py",
            "tests/gateway/test_telegram_rich_messages.py",
            "tests/gateway/test_telegram_overflow_partial.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.COMMENTARY,
        ("sendMessage",),
        (
            "gateway.stream_consumer_fallback:_send_commentary",
            "gateway.stream_consumer_fallback:_flush_segment_tail_on_edit_failure",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        None,
        RetryAmbiguity.NONE,
        "same chat/topic route; ``_interim_send`` must remain internal metadata",
        "one unit per commentary bubble; never let it seal or replace the turn final",
        (
            "tests/gateway/test_stream_final_contract.py",
            "tests/gateway/test_stream_consumer.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.CONTROL_PROMPT,
        ("sendMessage",),
        (
            "TelegramAdapter._send_message_with_thread_fallback",
            "TelegramAdapter._send_prompt",
            "TelegramAdapter.send_update_prompt",
            "TelegramAdapter._send_exec_approval_prompt",
            "TelegramAdapter.send_slash_confirm",
            "TelegramAdapter.send_clarify",
            "TelegramAdapter.send_model_picker",
            "TelegramAdapter.send_choice_picker",
            "TelegramAdapter._handle_slash_confirm_callback",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.URGENT,
        None,
        RetryAmbiguity.CONDITIONAL,
        "chat/topic/reply metadata; stale forum thread fallback may remove message_thread_id",
        "one unit per prompt; a definitive route/parse rejection may use its documented fallback",
        (
            "tests/gateway/test_telegram_approval_buttons.py",
            "tests/gateway/test_telegram_clarify_buttons.py",
            "tests/gateway/test_telegram_model_picker.py",
            "tests/gateway/test_telegram_inline_picker.py",
            "tests/gateway/test_telegram_slash_confirm.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.STATUS_UPDATE,
        ("sendMessage", "editMessageText"),
        (
            "TelegramAdapter.send_or_update_status",
            "gateway.run:_send_or_update_status_coro",
            "gateway.run_turn:_run_agent_notify_long_running",
            "gateway.run_notifications",
        ),
        OutboundDurability.REPLACEABLE,
        OutboundPriority.LOW,
        "bot/profile/chat/topic/status_key",
        RetryAmbiguity.AMBIGUOUS,
        "status key chooses one editable message; fresh send is only the existing caller fallback",
        "one unit per send/edit attempt; the status lane is not allowed to starve final text",
        (
            "tests/gateway/test_telegram_status_update.py",
            "tests/gateway/test_telegram_status_indicator.py",
            "tests/gateway/test_stream_consumer_fresh_final.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.PREVIEW_DELETE,
        ("deleteMessage",),
        (
            "TelegramAdapter.delete_message",
            "gateway.stream_consumer_transport:_delete_previews",
            "gateway.run_turn:_run_agent_schedule_bubble_cleanup",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/chat/message_id",
        RetryAmbiguity.NONE,
        "message id only; never delete the message just accepted as the replacement final",
        "one unit per delete; one bounded retry is allowed for the known false/flood cleanup case",
        (
            "tests/gateway/test_stream_consumer_fresh_final.py",
            "tests/gateway/test_stream_final_contract.py",
            "tests/gateway/test_telegram_final_delivery.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.TYPING,
        ("sendChatAction",),
        (
            "TelegramAdapter.send_typing",
            "TelegramAdapter._retrigger_typing",
            "gateway.run_turn_runner",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/chat/topic/typing",
        RetryAmbiguity.NONE,
        "chat + topic; DM-topic typing may retry once without message_thread_id",
        "one unit per action; transient failures arm the existing local cooldown",
        (
            "tests/gateway/test_telegram_typing_backoff.py",
            "tests/gateway/test_telegram_typing_retrigger.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.REACTION,
        ("setMessageReaction",),
        (
            "TelegramAdapter._set_reaction",
            "TelegramAdapter.on_processing_start",
            "TelegramAdapter.on_processing_complete",
            "tools.send_message_tool:_handle_react",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/chat/message_id/reaction",
        RetryAmbiguity.NONE,
        "normalized chat + message id; final reaction replaces the in-progress reaction",
        "one unit per reaction mutation; failures do not affect message delivery",
        (
            "tests/gateway/test_telegram_reactions.py",
            "tests/tools/test_send_message_react.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.TOPIC_CREATE,
        ("createForumTopic",),
        (
            "TelegramAdapter._create_dm_topic",
            "TelegramAdapter.create_handoff_thread",
            "TelegramAdapter.ensure_dm_topic",
            "TelegramAdapter._setup_dm_topics",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.HIGH,
        None,
        RetryAmbiguity.AMBIGUOUS,
        "private chat id + requested topic name; resulting thread id is persisted by the existing topic path",
        "one unit per create attempt; timeout must not blindly create a duplicate topic",
        (
            "tests/gateway/test_telegram_topic_mode.py",
            "tests/gateway/test_telegram_topic_profile_routing.py",
            "tests/gateway/test_telegram_prune_stale_topic_binding.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.TOPIC_RENAME,
        ("editForumTopic",),
        ("TelegramAdapter.rename_dm_topic",),
        OutboundDurability.DURABLE,
        OutboundPriority.NORMAL,
        None,
        RetryAmbiguity.AMBIGUOUS,
        "private chat id + numeric message_thread_id",
        "one unit per rename attempt; a timeout may have changed the remote name",
        ("tests/gateway/test_telegram_topic_mode.py",),
    ),
    TelegramMutationSpec(
        TelegramMutation.TOPIC_SEED,
        ("sendMessage",),
        ("TelegramAdapter._setup_dm_topics",),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        None,
        RetryAmbiguity.NONE,
        "configured private-chat message_thread_id",
        "one unit per seed bubble; topic creation/configuration remains the durable operation",
        ("tests/gateway/test_telegram_topic_mode.py",),
    ),
    TelegramMutationSpec(
        TelegramMutation.COMMAND_MENU,
        ("setMyCommands",),
        (
            "TelegramAdapter._register_command_menu",
            "TelegramAdapter._ensure_forum_commands",
        ),
        OutboundDurability.DURABLE,
        OutboundPriority.NORMAL,
        None,
        RetryAmbiguity.CONDITIONAL,
        "bot credential + BotCommand scope (default/private/group/forum chat)",
        "one unit per scope call; menu registration is recreated on connect/reconnect",
        ("tests/gateway/test_telegram_forum_commands.py",),
    ),
    TelegramMutationSpec(
        TelegramMutation.STATUS_INDICATOR,
        ("setMyShortDescription",),
        ("TelegramAdapter._set_status_indicator",),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/status-indicator",
        RetryAmbiguity.NONE,
        "bot credential only; no chat route",
        "one unit per online/offline update; optional presence UI must not delay disconnect",
        ("tests/gateway/test_telegram_status_indicator.py",),
    ),
    TelegramMutationSpec(
        TelegramMutation.WEBHOOK_CLEANUP,
        ("deleteWebhook",),
        ("TelegramAdapter._delete_webhook_best_effort",),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.NORMAL,
        "bot/profile/webhook-cleanup",
        RetryAmbiguity.CONDITIONAL,
        "bot credential only; cold start requires success, reconnects fail closed into polling recovery",
        "one unit per lifecycle attempt; not a user-message retry",
        (
            "tests/gateway/test_telegram_webhook_secret.py",
            "tests/gateway/test_telegram_connect.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.CALLBACK_ACK,
        ("answerCallbackQuery",),
        (
            "TelegramAdapter._handle_choice_picker_callback",
            "TelegramAdapter._handle_model_picker_callback",
            "TelegramAdapter._handle_exec_approval_callback",
            "TelegramAdapter._handle_slash_confirm_callback",
            "TelegramAdapter._handle_clarify_callback",
            "TelegramAdapter._handle_update_prompt_callback",
            "TelegramAdapter._handle_gmail_triage_callback",
            "TelegramAdapter._callback_authorized",
            "TelegramAdapter._claim_callback_state",
            "TelegramAdapter._picker_selection",
            "TelegramAdapter._parse_page",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/callback_query_id",
        RetryAmbiguity.NONE,
        "callback query id; answer promptly and do not replay a stale tap",
        "one unit per callback acknowledgement",
        (
            "tests/gateway/test_telegram_callback_auth_fail_closed.py",
            "tests/gateway/test_telegram_approval_buttons.py",
            "tests/gateway/test_telegram_clarify_buttons.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.CALLBACK_EDIT,
        ("editMessageText",),
        (
            "TelegramAdapter._edit_result_text",
            "TelegramAdapter._picker_edit",
            "TelegramAdapter._edit_html_quiet",
            "TelegramAdapter._edit_md_quiet",
            "TelegramAdapter._handle_model_picker_callback",
            "TelegramAdapter._handle_gmail_triage_callback",
        ),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.LOW,
        "bot/profile/chat/message_id/picker-or-callback-lane",
        RetryAmbiguity.NONE,
        "callback message id; stale picker edits may be abandoned",
        "one unit per UI repaint; never block a durable prompt or final response",
        (
            "tests/gateway/test_telegram_model_picker.py",
            "tests/gateway/test_telegram_inline_picker.py",
            "tests/gateway/test_telegram_clarify_buttons.py",
        ),
    ),
    TelegramMutationSpec(
        TelegramMutation.INLINE_ANSWER,
        ("answerInlineQuery",),
        ("TelegramAdapter._handle_inline_query",),
        OutboundDurability.BEST_EFFORT,
        OutboundPriority.NORMAL,
        "bot/profile/inline_query_id",
        RetryAmbiguity.NONE,
        "inline query id; response is scoped to Telegram's query lifetime",
        "one unit per inline query answer; expired queries are not replayed",
        ("tests/gateway/test_telegram_inline_picker.py",),
    ),
)


__all__ = [
    "OutboundDurability",
    "OutboundPriority",
    "RetryAmbiguity",
    "TelegramMutation",
    "TelegramMutationSpec",
    "TelegramOutboundMetadata",
    "TELEGRAM_MUTATION_CATALOG",
]
