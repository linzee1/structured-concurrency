#!/usr/bin/env python3
"""Fail when an English Markdown document contains Han characters."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
HAN = re.compile(r"[\u3400-\u9fff]")


def is_english_doc(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    parts = tuple(part.lower() for part in relative.parts)
    name = path.name.lower()
    return "en" in parts or name.endswith(".en.md") or name.endswith("_en.md")


violations = []
for document in ROOT.rglob("*.md"):
    if not is_english_doc(document):
        continue
    for line_number, line in enumerate(document.read_text(encoding="utf-8").splitlines(), 1):
        if HAN.search(line):
            violations.append(f"{document.relative_to(ROOT)}:{line_number}:{line}")

if violations:
    print("English documentation contains Han characters:", file=sys.stderr)
    print("\n".join(violations), file=sys.stderr)
    sys.exit(1)
