"""
Jobs-oprydning: den in-memory jobs-dict må ikke vokse ubegrænset. Tester TTL-prune
af gamle terminale jobs og antals-cap (MAX_JOBS).
"""

import time

import main


def test_prune_removes_old_terminal_jobs(monkeypatch):
    monkeypatch.setattr(main, "JOB_RETENTION_SECONDS", 10)
    main.jobs.clear()
    main.jobs["old"] = {"status": "done", "created_ts": time.time() - 100}
    main.jobs["recent"] = {"status": "done", "created_ts": time.time()}
    main.jobs["running"] = {"status": "analyzing", "created_ts": time.time() - 100}
    try:
        main._prune_jobs()
        assert "old" not in main.jobs       # gammelt + terminalt -> fjernet
        assert "recent" in main.jobs        # nyt -> beholdt
        assert "running" in main.jobs       # ikke terminalt -> beholdt selv om gammelt
    finally:
        main.jobs.clear()


def test_prune_caps_max_jobs(monkeypatch):
    monkeypatch.setattr(main, "MAX_JOBS", 3)
    main.jobs.clear()
    base = time.time()
    for i in range(10):
        main.jobs[f"j{i}"] = {"status": "done", "created_ts": base + i}  # alle nye -> ingen TTL-prune
    try:
        main._prune_jobs()
        assert len(main.jobs) == 3
        assert set(main.jobs) == {"j7", "j8", "j9"}  # de nyeste beholdes
    finally:
        main.jobs.clear()
