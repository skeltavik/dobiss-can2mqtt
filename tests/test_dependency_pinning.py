"""Regression tests for immutable CI and dependency policy."""
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_pinned_dependencies.py"
SPEC = importlib.util.spec_from_file_location("pinning_policy", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
policy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(policy)


def configure_root(monkeypatch, tmp_path):
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "WORKFLOWS", workflows)
    return workflows


def test_rejects_mutable_action_tag(monkeypatch, tmp_path):
    workflows = configure_root(monkeypatch, tmp_path)
    (workflows / "ci.yml").write_text("steps:\n  - uses: actions/checkout@v4\n")
    errors = []
    policy.check_actions(errors)
    assert any("40-character commit SHA" in error for error in errors)


def test_rejects_unhashed_dependency(monkeypatch, tmp_path):
    configure_root(monkeypatch, tmp_path)
    (tmp_path / "requirements.txt").write_text("pyyaml==6.0.3\n")
    errors = []
    policy.check_python_requirements(errors)
    assert any("lacks a sha256 hash" in error for error in errors)


def test_accepts_sha_pinned_action_and_hashed_dependency(monkeypatch, tmp_path):
    workflows = configure_root(monkeypatch, tmp_path)
    sha = "a" * 40
    digest = "b" * 64
    (workflows / "ci.yml").write_text(f"steps:\n  - uses: actions/checkout@{sha}\n")
    (tmp_path / "requirements.txt").write_text(
        f"pyyaml==6.0.3 \\\n    --hash=sha256:{digest}\n"
    )
    errors = []
    policy.check_actions(errors)
    policy.check_python_requirements(errors)
    assert errors == []
