"""Unit tests for the GitHub Actions status helper; no network required."""
from __future__ import annotations

from scripts import ci_status
from scripts.ci_status import CiStatusError, Run, main, parse_repo, select_runs


def _run(sha: str, conclusion: str, name: str = "CI") -> Run:
    return Run(
        name=name,
        head_sha=sha,
        status="completed",
        conclusion=conclusion,
        created_at="2026-10-22T07:35:03Z",
        html_url=f"https://example.test/{sha}",
    )


def test_parse_repo_accepts_ssh_and_https_remotes():
    assert parse_repo("git@github.com:Hezhero/daily_stock_analysis.git") == (
        "Hezhero/daily_stock_analysis"
    )
    assert parse_repo("https://github.com/Hezhero/daily_stock_analysis.git") == (
        "Hezhero/daily_stock_analysis"
    )
    assert parse_repo("ssh://git@github.com/Hezhero/daily_stock_analysis.git") == (
        "Hezhero/daily_stock_analysis"
    )


def test_parse_repo_rejects_non_github_remotes():
    assert parse_repo("https://gitlab.com/owner/repo.git") is None
    assert parse_repo("") is None


def test_select_runs_filters_by_sha_and_workflow():
    runs = [
        _run("aaa", "success"),
        _run("aaa", "failure", name="Daily"),
        _run("bbb", "success"),
    ]

    assert select_runs(runs, "aaa", "CI") == [runs[0]]
    assert select_runs(runs, "aaa", None) == [runs[0], runs[1]]
    assert select_runs(runs, "ccc", "CI") == []


def test_main_returns_zero_for_a_successful_run(monkeypatch, capsys):
    monkeypatch.setattr(ci_status, "fetch_runs", lambda repo, branch: [_run("abc123", "success")])
    monkeypatch.setattr(ci_status, "fetch_check_runs", lambda repo, sha: 1)

    exit_code = main(["--repo", "owner/repo", "--branch", "hzh", "--sha", "abc123"])

    assert exit_code == 0
    assert "conclusion=success" in capsys.readouterr().out


def test_main_returns_one_when_head_has_no_run(monkeypatch, capsys):
    monkeypatch.setattr(ci_status, "fetch_runs", lambda repo, branch: [_run("old", "success")])
    monkeypatch.setattr(ci_status, "fetch_check_runs", lambda repo, sha: 0)

    exit_code = main(["--repo", "owner/repo", "--branch", "hzh", "--sha", "abc123"])

    assert exit_code == 1
    assert "NO RUN for abc123" in capsys.readouterr().out


def test_main_returns_one_when_the_only_run_failed(monkeypatch):
    monkeypatch.setattr(ci_status, "fetch_runs", lambda repo, branch: [_run("abc123", "failure")])
    monkeypatch.setattr(ci_status, "fetch_check_runs", lambda repo, sha: 1)

    assert main(["--repo", "owner/repo", "--branch", "hzh", "--sha", "abc123"]) == 1


def test_main_returns_zero_when_any_run_succeeded(monkeypatch):
    monkeypatch.setattr(
        ci_status,
        "fetch_runs",
        lambda repo, branch: [_run("abc123", "failure"), _run("abc123", "success")],
    )
    monkeypatch.setattr(ci_status, "fetch_check_runs", lambda repo, sha: 2)

    assert main(["--repo", "owner/repo", "--branch", "hzh", "--sha", "abc123"]) == 0


def test_main_returns_two_on_api_error(monkeypatch, capsys):
    def fail(repo, branch):
        raise CiStatusError("rate limit reached")

    monkeypatch.setattr(ci_status, "fetch_runs", fail)

    assert main(["--repo", "owner/repo", "--branch", "hzh", "--sha", "abc123"]) == 2
    assert "rate limit reached" in capsys.readouterr().err


def test_main_can_ignore_the_workflow_filter(monkeypatch):
    monkeypatch.setattr(
        ci_status,
        "fetch_runs",
        lambda repo, branch: [_run("abc123", "success", name="Daily")],
    )
    monkeypatch.setattr(ci_status, "fetch_check_runs", lambda repo, sha: 1)

    assert main(["--repo", "owner/repo", "--branch", "main", "--sha", "abc123", "--workflow", ""]) == 0
