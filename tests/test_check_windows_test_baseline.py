"""Unit tests for the native-Windows baseline checker; no pytest run required."""
from __future__ import annotations

import textwrap
from pathlib import Path

from scripts import check_windows_test_baseline as baseline_module
from scripts.check_windows_test_baseline import (
    BASELINE_HEADER,
    TODO,
    Entry,
    Outcome,
    _write_baseline,
    compare,
    main,
    parse_baseline,
    read_junit,
    render_baseline,
)


def _junit(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "report.xml"
    path.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>", encoding="utf-8")
    return path


def test_parse_baseline_reads_table_rows_only():
    text = BASELINE_HEADER + "| `tests.a::test_x` | product-excluded | Codex 后端在 Windows 被排除 |\n"
    assert parse_baseline(text) == [
        Entry(
            test="tests.a::test_x",
            category="product-excluded",
            reason="Codex 后端在 Windows 被排除",
        )
    ]


def test_parse_baseline_ignores_header_and_prose():
    assert parse_baseline(BASELINE_HEADER) == []


def test_render_baseline_round_trips():
    entries = [Entry("tests.b::test_y", "stdlib-missing", "time.tzset 仅 POSIX")]
    assert parse_baseline(render_baseline(entries)) == entries


def test_read_junit_separates_collection_errors(tmp_path):
    path = _junit(
        tmp_path,
        textwrap.dedent(
            """
            <testcase classname="tests.a" name="test_x"><failure/></testcase>
            <testcase classname="tests.b" name="test_y"><error/></testcase>
            <testcase classname="tests.c" name="tests.c"><error/></testcase>
            <testcase classname="tests.d" name="test_z"/>
            """
        ),
    )

    outcome = read_junit(path)

    assert outcome.failures == {"tests.a::test_x", "tests.b::test_y"}
    assert outcome.collection_errors == {"tests.c::tests.c"}


def test_compare_reports_new_and_stale():
    outcome = Outcome(failures={"tests.a::test_x", "tests.new::test_n"})
    report = compare(
        outcome,
        [
            Entry("tests.a::test_x", "product-excluded", "ok"),
            Entry("tests.gone::test_g", "stdlib-missing", "gone"),
        ],
    )

    assert report.new == ["tests.new::test_n"]
    assert report.stale == ["tests.gone::test_g"]
    assert report.exit_code() == 1


def test_compare_flags_unresolved_reason():
    outcome = Outcome(failures={"tests.a::test_x"})
    report = compare(outcome, [Entry("tests.a::test_x", TODO, TODO)])

    assert report.unresolved == ["tests.a::test_x"]
    assert report.exit_code() == 1


def test_compare_flags_category_outside_the_contract():
    outcome = Outcome(failures={"tests.a::test_x"})
    report = compare(outcome, [Entry("tests.a::test_x", "flaky", "随便写")])

    assert report.bad_category == ["tests.a::test_x (flaky)"]
    assert report.exit_code() == 1


def test_compare_is_clean_when_sets_match():
    outcome = Outcome(failures={"tests.a::test_x"})
    report = compare(outcome, [Entry("tests.a::test_x", "product-excluded", "ok")])

    assert report.ok is True
    assert report.exit_code() == 0


def test_compare_treats_collection_error_as_fatal():
    outcome = Outcome(collection_errors={"tests.c::tests.c"})
    report = compare(outcome, [])

    assert report.collection_errors == ["tests.c::tests.c"]
    assert report.exit_code() == 1


def test_update_preserves_reasons_and_marks_new_entries_todo(tmp_path):
    path = tmp_path / "baseline.md"
    path.write_text(
        render_baseline([Entry("tests.a::test_x", "product-excluded", "已经归因")]),
        encoding="utf-8",
    )

    written = {entry.test: entry for entry in _write_baseline(path, Outcome(failures={"tests.a::test_x", "tests.new::test_n"}))}

    assert written["tests.a::test_x"].reason == "已经归因"
    assert written["tests.a::test_x"].category == "product-excluded"
    assert written["tests.new::test_n"].reason == TODO
    assert written["tests.new::test_n"].category == TODO


def test_posix_refuses_a_non_empty_baseline(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline.md"
    baseline.write_text(
        render_baseline([Entry("tests.a::test_x", "posix-mode-bits", "mode 位仅 POSIX")]),
        encoding="utf-8",
    )
    monkeypatch.setattr(baseline_module, "_is_native_windows", lambda: False)

    assert main(["--baseline", str(baseline)]) == 2


def test_update_refuses_while_collection_errors_exist(tmp_path):
    junit = _junit(tmp_path, '<testcase classname="tests.c" name="tests.c"><error/></testcase>')
    baseline = tmp_path / "baseline.md"

    exit_code = main(["--baseline", str(baseline), "--junit", str(junit), "--update"])

    assert exit_code == 1
    assert not baseline.exists()

def test_known_unattributed_category_is_accepted():
    outcome = Outcome(failures={"tests.a::test_x"})
    report = compare(outcome, [Entry("tests.a::test_x", "known-unattributed", "既有失败，见票 12")])

    assert report.ok is True
    assert report.exit_code() == 0

def test_known_unattributed_entries_that_pass_do_not_fail_the_run():
    outcome = Outcome(failures=set())
    report = compare(outcome, [Entry("tests.a::test_x", "known-unattributed", "既有失败")])

    assert report.ok is True
    assert report.stale == []
    assert report.stale_soft == ["tests.a::test_x"]
