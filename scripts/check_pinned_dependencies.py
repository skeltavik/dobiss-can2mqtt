#!/usr/bin/env python3
"""Fail closed when external CI actions or dependency installs are not hash-pinned."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTION_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
USES = re.compile(r"^\s*-?\s*uses:\s*([^\s#]+)", re.MULTILINE)
PIP_INSTALL = re.compile(r"\b(?:python(?:3(?:\.\d+)?)?\s+-m\s+)?pip(?:3)?\s+install\b([^\n]*)", re.IGNORECASE)
HASH = re.compile(r"--hash=sha256:[0-9a-fA-F]{64}\b")


def check_actions(errors: list[str]) -> None:
    for workflow in sorted(WORKFLOWS.glob("*.y*ml")):
        text = workflow.read_text(encoding="utf-8")
        for match in USES.finditer(text):
            value = match.group(1).strip('"\'')
            if value.startswith("./"):
                continue  # Local actions are part of the reviewed repository tree.
            if value.startswith("docker://"):
                ref = value.removeprefix("docker://")
                if "@" not in ref or not DIGEST.fullmatch(ref.rsplit("@", 1)[1]):
                    errors.append(f"{workflow.relative_to(ROOT)}: Docker action is not pinned by sha256 digest: {value}")
                continue
            if "@" not in value or not ACTION_SHA.fullmatch(value.rsplit("@", 1)[1]):
                errors.append(f"{workflow.relative_to(ROOT)}: GitHub Action is not pinned to a 40-character commit SHA: {value}")
        for match in PIP_INSTALL.finditer(text):
            args = match.group(1)
            if "--require-hashes" not in args:
                errors.append(f"{workflow.relative_to(ROOT)}: pip install must use --require-hashes")


def logical_requirements(path: Path) -> list[str]:
    records: list[str] = []
    current = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        current = f"{current} {line}".strip()
        if current.endswith("\\"):
            current = current[:-1].strip()
            continue
        records.append(current)
        current = ""
    if current:
        records.append(current)
    return records


def check_python_requirements(errors: list[str]) -> None:
    for path in sorted(ROOT.glob("**/requirements*.txt")):
        if ".git" in path.parts:
            continue
        records = logical_requirements(path)
        for record in records:
            if record.startswith(("-r ", "--requirement ", "--require-hashes", "--index-url", "--extra-index-url")):
                continue
            effective = record.split("#", 1)[0].rstrip()
            if effective.startswith(("-e ", "--editable ", "git+", "http://", "https://")):
                errors.append(f"{path.relative_to(ROOT)}: editable/VCS/URL dependency is forbidden: {record}")
                continue
            if "==" not in effective:
                errors.append(f"{path.relative_to(ROOT)}: dependency must use an exact == version: {record}")
            if not HASH.search(effective):
                errors.append(f"{path.relative_to(ROOT)}: dependency lacks a sha256 hash: {record}")


def check_javascript_manifests(errors: list[str]) -> None:
    for manifest in sorted(ROOT.glob("**/package.json")):
        if any(part in {".git", "node_modules"} for part in manifest.parts):
            continue
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        deps = any(payload.get(key) for key in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"))
        if not deps:
            continue
        directory = manifest.parent
        lockfiles = [directory / "package-lock.json", directory / "npm-shrinkwrap.json", directory / "pnpm-lock.yaml", directory / "yarn.lock"]
        if not any(lock.is_file() for lock in lockfiles):
            errors.append(f"{manifest.relative_to(ROOT)}: dependencies require a committed integrity-bearing lockfile")


def main() -> int:
    errors: list[str] = []
    check_actions(errors)
    check_python_requirements(errors)
    check_javascript_manifests(errors)
    if errors:
        print("Dependency pinning policy violations:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("dependency/action pinning policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
