"""Durable logical-session context synchronization regression tests."""

from hermes_state import SessionDB
from gateway.platforms.api_server import APIServerAdapter


def test_queued_context_turn_accepts_lock_owner_revision_advance_only():
    assert APIServerAdapter._context_turn_revision(4, 4, waited_for_lock=False) == 4
    assert APIServerAdapter._context_turn_revision(4, 5, waited_for_lock=True) == 5
    assert APIServerAdapter._context_turn_revision(4, 5, waited_for_lock=False) is None
    assert APIServerAdapter._context_turn_revision(4, 3, waited_for_lock=True) is None


def test_context_state_cas_in_place_compression_and_bounded_outbox(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    try:
        initial = db.get_or_create_context_state("logical", effective_session_id="physical")
        assert initial["revision"] == 0

        # In-place compression advances epoch exactly once even with same ID.
        advanced = db.advance_context_state(
            "logical", expected_revision=0, effective_session_id="physical",
            event_type="session:compress", compressed=True,
        )
        assert advanced["revision"] == 1
        assert advanced["compression_epoch"] == 1
        assert advanced["effective_session_id"] == "physical"
        assert db.advance_context_state(
            "logical", expected_revision=0, effective_session_id="other",
        ) is None

        # The public cursor outbox has a deterministic retention gap.
        for revision in range(1, 1002):
            state = db.advance_context_state(
                "logical", expected_revision=revision, effective_session_id="physical",
            )
            assert state is not None
        minimum, maximum = db.context_event_bounds("logical")
        assert minimum is not None and maximum is not None
        assert maximum - minimum + 1 == 1000
        assert len(db.list_context_events("logical", after=0, limit=2000)) == 200
    finally:
        db.close()


def test_compression_lifecycle_events_are_durable_without_advancing_revision(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    try:
        logical = "logical-lifecycle"
        db.get_or_create_context_state(logical)
        started = db.append_context_event(
            logical,
            event_type="session:compress_started",
            telemetry={"operationId": "ctxop_" + "a" * 32, "reason": "threshold"},
        )
        failed = db.append_context_event(
            logical,
            event_type="session:compress_failed",
            telemetry={"operationId": "ctxop_" + "a" * 32, "reason": "no_progress", "duration": 0.25},
        )
        state = db.get_or_create_context_state(logical)
        assert started["revision"] == failed["revision"] == state["revision"] == 0
        assert started["compression_epoch"] == failed["compression_epoch"] == state["compression_epoch"] == 0
        assert [event["event_type"] for event in db.list_context_events(logical)] == [
            "session:compress_started",
            "session:compress_failed",
        ]
    finally:
        db.close()
