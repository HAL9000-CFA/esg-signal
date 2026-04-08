import json

import pytest

import pipeline.audit_log as al


@pytest.fixture(autouse=True)
def tmp_log(tmp_path, monkeypatch):
    log_file = tmp_path / "audit_log.jsonl"
    monkeypatch.setattr(al, "LOG_PATH", log_file)
    return log_file


def test_log_writes_record(tmp_log):
    al.log_llm_call(
        agent="test_agent",
        model="claude-opus-4-5",
        purpose="unit_test",
        input_tokens=100,
        output_tokens=50,
        cost_usd=0.001,
        temperature=0.0,
    )
    lines = tmp_log.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["agent"] == "test_agent"
    assert record["total_tokens"] == 150
    assert record["cached"] is False


def test_summarise_aggregates_correctly(tmp_log):
    for _ in range(3):
        al.log_llm_call(
            agent="scorer",
            model="claude-opus-4-5",
            purpose="test",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.05,
            run_id="run-abc",
            temperature=0.0,
        )
    summary = al.summarise("run-abc")
    assert summary["total_calls"] == 3
    assert summary["total_cost_usd"] == pytest.approx(0.15)


def test_compute_cost_known_model():
    cost = al.compute_cost("claude-opus-4-5", input_tokens=1_000_000, output_tokens=1_000_000)
    assert cost == pytest.approx(30.0)


def test_compute_cost_unknown_model():
    assert al.compute_cost("unknown-model", input_tokens=500, output_tokens=500) == 0.0


def test_summarise_empty_log(tmp_log):
    assert al.summarise() == {}


def test_log_is_thread_safe(tmp_log):
    import threading

    threads = [
        threading.Thread(
            target=al.log_llm_call,
            kwargs={
                "agent": "scorer",
                "model": "claude-opus-4-5",
                "purpose": "test",
                "input_tokens": 10,
                "output_tokens": 10,
                "cost_usd": 0.001,
                "temperature": 0.0,
            },
        )
        for _ in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lines = tmp_log.read_text().splitlines()
    assert len(lines) == 20
