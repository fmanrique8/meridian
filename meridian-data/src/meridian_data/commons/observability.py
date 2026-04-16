"""Reusable observability helpers for structured key-value logs."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).isoformat()


def log_kv_event(  # noqa: PLR0913
    *,
    logger: logging.Logger,
    pipeline: str,
    source: str,
    level: int,
    event: str,
    node: str,
    run_date: date | None,
    status: str,
    row_count: int | None = None,
    entity_count: int | None = None,
    duration_ms: int | None = None,
    **extra_fields: Any,
) -> None:
    """Emit a standardized key-value observability event."""
    fields: dict[str, Any] = {
        "event": event,
        "pipeline": pipeline,
        "source": source,
        "node": node,
        "run_date": run_date.isoformat() if run_date is not None else None,
        "row_count": row_count,
        "entity_count": entity_count,
        "duration_ms": duration_ms,
        "status": status,
        **extra_fields,
    }
    payload = " ".join(
        f"{key}={_format_log_value(value)}" for key, value in fields.items()
    )
    logger.log(level, payload)


def build_log_kv_emitter(
    *,
    logger: logging.Logger,
    pipeline: str,
    source: str,
) -> Callable[..., None]:
    """Create a pre-bound emitter for one pipeline/source pair."""

    def _emit(  # noqa: PLR0913
        *,
        level: int,
        event: str,
        node: str,
        run_date: date | None,
        status: str,
        row_count: int | None = None,
        entity_count: int | None = None,
        duration_ms: int | None = None,
        **extra_fields: Any,
    ) -> None:
        log_kv_event(
            logger=logger,
            pipeline=pipeline,
            source=source,
            level=level,
            event=event,
            node=node,
            run_date=run_date,
            status=status,
            row_count=row_count,
            entity_count=entity_count,
            duration_ms=duration_ms,
            **extra_fields,
        )

    return _emit


def _format_log_value(value: Any) -> str:
    """Format key-value log field values consistently."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
