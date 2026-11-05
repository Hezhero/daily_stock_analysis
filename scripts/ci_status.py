#!/usr/bin/env python3
"""Report whether GitHub Actions has a run for the current commit.

The repository is public, so this works without ``gh`` or a token. It is the
fallback for the "PR / CI evidence" step when ``gh`` is unavailable, and it
turns a silent "pending CI" state into an explicit failure.

Exit codes: ``0`` a successful run exists for HEAD, ``1`` no successful run for
HEAD, ``2`` usage or network error.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional, Sequence

DEFAULT_REPO = "Hezhero/daily_stock_analysis"
DEFAULT_WORKFLOW = "CI"
API_ROOT = "https://api.github.com"
TIMEOUT_SECONDS = 20


class CiStatusError(RuntimeError):
    """Raised when the GitHub API cannot be queried."""


@dataclass(frozen=True)
class Run:
    name: str
    head_sha: str
    status: str
    conclusion: Optional[str]
    created_at: str
    html_url: str


def _git(*args: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def parse_repo(remote: str) -> Optional[str]:
    """Extract ``owner/repo`` from an SSH or HTTPS GitHub remote URL."""
    remote = remote.strip()
    patterns = (
        r"^git@github\.com:(?P<repo>[^/]+/[^/]+?)(?:\.git)?$",
        r"^https?://github\.com/(?P<repo>[^/]+/[^/]+?)(?:\.git)?$",
        r"^ssh://git@github\.com/(?P<repo>[^/]+/[^/]+?)(?:\.git)?$",
    )
    for pattern in patterns:
        match = re.match(pattern, remote)
        if match:
            return match.group("repo")
    return None


def resolve_repo(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    remote = _git("remote", "get-url", "origin")
    parsed = parse_repo(remote) if remote else None
    return parsed or DEFAULT_REPO


def resolve_branch(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return _git("rev-parse", "--abbrev-ref", "HEAD") or "HEAD"


def _request_json(url: str) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "dsa-ci-status",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        if exc.code == 403 and "rate limit" in detail.lower():
            raise CiStatusError(
                "GitHub API rate limit reached; set GITHUB_TOKEN or retry later"
            ) from exc
        raise CiStatusError(f"HTTP {exc.code} for {url}\n{detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise CiStatusError(f"cannot reach {url}: {exc.reason}") from exc


def fetch_runs(repo: str, branch: str) -> list[Run]:
    url = (
        f"{API_ROOT}/repos/{repo}/actions/runs"
        f"?branch={urllib.parse.quote(branch)}&per_page=100"
    )
    payload = _request_json(url)
    runs = []
    for item in payload.get("workflow_runs", []):
        runs.append(
            Run(
                name=item.get("name", ""),
                head_sha=item.get("head_sha", ""),
                status=item.get("status", ""),
                conclusion=item.get("conclusion"),
                created_at=item.get("created_at", ""),
                html_url=item.get("html_url", ""),
            )
        )
    return runs


def fetch_check_runs(repo: str, sha: str) -> int:
    url = f"{API_ROOT}/repos/{repo}/commits/{sha}/check-runs"
    payload = _request_json(url)
    return int(payload.get("total_count", 0))


def select_runs(runs: Sequence[Run], sha: str, workflow: Optional[str]) -> list[Run]:
    selected = [run for run in runs if run.head_sha == sha]
    if workflow:
        selected = [run for run in selected if run.name == workflow]
    return selected


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", help="owner/repo; defaults to the origin remote")
    parser.add_argument("--branch", help="branch to query; defaults to the current branch")
    parser.add_argument("--sha", help="commit SHA; defaults to HEAD")
    parser.add_argument(
        "--workflow",
        default=DEFAULT_WORKFLOW,
        help="workflow name filter; pass an empty string to accept any workflow",
    )
    parser.add_argument("--json", action="store_true", help="print the selected runs as JSON")
    args = parser.parse_args(argv)

    repo = resolve_repo(args.repo)
    branch = resolve_branch(args.branch)
    sha = args.sha or _git("rev-parse", "HEAD")
    if not sha:
        print("ci-status: cannot resolve HEAD; run inside the repository", file=sys.stderr)
        return 2
    workflow = args.workflow or None

    try:
        runs = fetch_runs(repo, branch)
        check_run_count = fetch_check_runs(repo, sha)
    except CiStatusError as exc:
        print(f"ci-status: {exc}", file=sys.stderr)
        return 2

    selected = select_runs(runs, sha, workflow)

    print(f"ci-status: repo={repo} branch={branch} sha={sha[:12]}")
    print(f"ci-status: workflow={workflow or 'all'} check-runs={check_run_count}")

    if not selected:
        suffix = f" (workflow {workflow})" if workflow else ""
        print(f"ci-status: NO RUN for {sha[:12]} on {branch}{suffix}")
        print(
            "ci-status: pushes are not producing workflow runs; "
            "check GitHub Actions/billing settings"
        )
        return 1

    for run in selected:
        print(
            f"ci-status: {run.name} status={run.status} conclusion={run.conclusion} "
            f"created={run.created_at} {run.html_url}"
        )
    if args.json:
        print(json.dumps([run.__dict__ for run in selected], ensure_ascii=False, indent=2))

    return 0 if any(run.conclusion == "success" for run in selected) else 1


if __name__ == "__main__":
    sys.exit(main())
