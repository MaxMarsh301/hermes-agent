"""Tests for the store-level CAS fire claim (Phase 4C).

`claim_job_for_fire` gives multi-machine at-most-once semantics when an external
scheduler (Chronos) fires a job: across N gateway replicas, exactly ONE wins the
claim for a given fire. Single-machine deployments always win (unaffected).

These exercise the real store against a temp HERMES_HOME (no mocks) per the
E2E-over-mocks discipline for file-touching code.
"""
import json

import pytest

import cron.jobs as jobs


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    """Isolate cron storage even when cron.jobs was imported before setup."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with jobs.use_cron_store(tmp_path):
        yield tmp_path


def test_temp_home_routes_preimported_jobs_module_away_from_its_default_store(
    temp_home, tmp_path, monkeypatch
):
    """A cached cron.jobs module must not write its import-time default store."""
    cached_default_home = tmp_path / "cached-default"
    cached_cron_dir = cached_default_home / "cron"
    cached_jobs_file = cached_cron_dir / "jobs.json"
    monkeypatch.setattr(jobs, "HERMES_DIR", cached_default_home)
    monkeypatch.setattr(jobs, "CRON_DIR", cached_cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cached_jobs_file)
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cached_cron_dir / "output")

    sentinel = [{"id": "must-not-change"}]
    cached_jobs_file.parent.mkdir(parents=True, exist_ok=True)
    cached_jobs_file.write_text(json.dumps(sentinel), encoding="utf-8")

    created = jobs.create_job(prompt="x", schedule="every 5m", name="isolated")

    assert json.loads(cached_jobs_file.read_text(encoding="utf-8")) == sentinel
    isolated_jobs_file = temp_home / "cron" / "jobs.json"
    isolated_store = json.loads(isolated_jobs_file.read_text(encoding="utf-8"))
    assert created["id"] in {job["id"] for job in isolated_store["jobs"]}


def test_claim_succeeds_once_then_blocks(temp_home):
    """First claim for a fire wins; a second claim for the same fire loses, and
    next_run_at is advanced (a re-delivery for the old time can't re-fire)."""
    from cron.jobs import create_job, claim_job_for_fire, get_job

    job = create_job(prompt="x", schedule="every 5m", name="t")
    jid = job["id"]
    before = get_job(jid)["next_run_at"]

    assert claim_job_for_fire(jid) is True
    assert claim_job_for_fire(jid) is False
    assert get_job(jid)["next_run_at"] != before


def test_claim_oneshot_cannot_be_double_claimed(temp_home):
    """A one-shot can't be double-claimed (the fresh claim blocks the retry)."""
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="30m", name="o")
    assert claim_job_for_fire(job["id"]) is True
    assert claim_job_for_fire(job["id"]) is False


def test_claim_unknown_job_returns_false(temp_home):
    from cron.jobs import claim_job_for_fire

    assert claim_job_for_fire("nope-does-not-exist") is False


def test_claim_paused_job_returns_false(temp_home):
    """A paused job can't be claimed."""
    from cron.jobs import create_job, claim_job_for_fire, pause_job

    job = create_job(prompt="x", schedule="every 5m", name="p")
    pause_job(job["id"])
    assert claim_job_for_fire(job["id"]) is False


def test_stale_claim_is_reclaimable(temp_home, monkeypatch):
    """A claim older than the TTL is overwritten — the fire isn't stuck forever
    if the winning machine crashed before mark_job_run cleared the claim."""
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="s")
    jid = job["id"]
    assert claim_job_for_fire(jid) is True
    # With a 0s TTL, the existing claim is always considered stale.
    assert claim_job_for_fire(jid, claim_ttl_seconds=0) is True


def test_mark_job_run_clears_claim(temp_home):
    """After a recurring job completes, its claim is cleared so the next fire
    can be claimed again."""
    from cron.jobs import create_job, claim_job_for_fire, mark_job_run, get_job

    job = create_job(prompt="x", schedule="every 5m", name="c")
    jid = job["id"]
    assert claim_job_for_fire(jid) is True
    assert get_job(jid).get("fire_claim") is not None

    mark_job_run(jid, success=True)
    assert get_job(jid).get("fire_claim") is None
    # …and the re-armed recurring job is claimable again.
    assert claim_job_for_fire(jid) is True


def test_consumed_oneshot_is_not_due_after_simulated_restart(temp_home):
    """Crash after a remote accept cannot cause a fresh agent execution."""
    from cron.jobs import consume_one_shot_for_run, create_job, get_due_jobs, get_job

    job = create_job(prompt="x", schedule="30m", name="once")
    # This is the write performed before the agent/network path starts.
    assert consume_one_shot_for_run(job["id"]) is True
    # A restarted scheduler re-reads jobs.json and sees no runnable one-shot.
    assert job["id"] not in {candidate["id"] for candidate in get_due_jobs()}
    stored = get_job(job["id"])
    assert stored is not None
    assert stored["state"] == "consumed"
    assert stored["enabled"] is False
