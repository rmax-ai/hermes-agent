"""Contract tests for the issue #2 Telegram outbound mutation inventory.

These tests validate metadata relationships, not the source layout of the adapter.
"""

import pytest

from gateway.telegram_outbound_contract import (
    OutboundDurability,
    OutboundPriority,
    RetryAmbiguity,
    TELEGRAM_MUTATION_CATALOG,
    TelegramMutation,
    TelegramOutboundMetadata,
)


def test_metadata_requires_coalescing_only_for_replaceable_work() -> None:
    replaceable = TelegramOutboundMetadata(
        operation=TelegramMutation.STREAM_DRAFT,
        priority=OutboundPriority.LOW,
        coalescing_key="bot/profile/chat/topic/turn",
        durability=OutboundDurability.REPLACEABLE,
        retry_ambiguity=RetryAmbiguity.NONE,
    )
    assert replaceable.coalescing_key == "bot/profile/chat/topic/turn"

    with pytest.raises(ValueError, match="durable"):
        TelegramOutboundMetadata(
            operation=TelegramMutation.FINAL_TEXT,
            priority=OutboundPriority.HIGH,
            coalescing_key="chat/topic/final",
            durability=OutboundDurability.DURABLE,
            retry_ambiguity=RetryAmbiguity.CONDITIONAL,
        )

    with pytest.raises(ValueError, match="replaceable"):
        TelegramOutboundMetadata(
            operation=TelegramMutation.STATUS_UPDATE,
            priority=OutboundPriority.LOW,
            coalescing_key=None,
            durability=OutboundDurability.REPLACEABLE,
            retry_ambiguity=RetryAmbiguity.AMBIGUOUS,
        )


def test_catalog_rows_materialize_valid_metadata() -> None:
    assert TELEGRAM_MUTATION_CATALOG
    assert len({spec.operation for spec in TELEGRAM_MUTATION_CATALOG}) == len(
        TELEGRAM_MUTATION_CATALOG
    )

    for spec in TELEGRAM_MUTATION_CATALOG:
        key = (
            f"bot/profile/chat/{spec.operation.value}"
            if spec.coalescing_scope
            else None
        )
        metadata = spec.metadata(coalescing_key=key)
        assert metadata.operation is spec.operation
        assert metadata.priority is spec.priority
        assert metadata.durability is spec.durability
        assert metadata.retry_ambiguity is spec.retry_ambiguity
        assert spec.api_methods
        assert spec.call_sites
        assert spec.routing
        assert spec.budget
        assert spec.invariant_tests
