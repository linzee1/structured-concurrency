#!/usr/bin/env python3
"""Check that relative Markdown links point to files or directories in the repo."""

from __future__ import annotations

import re
import posixpath
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (ROOT / "README.md", ROOT / "README.en.md", ROOT / "CHANGELOG.md", ROOT / "docs", ROOT / "demo")
LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+['\"][^)]*['\"])?\)")


def markdown_files(path: Path):
    if path.is_file():
        yield path
    elif path.is_dir():
        yield from path.rglob("*.md")


def target_exists(source: Path, path: Path, raw_path: str) -> bool:
    if path.exists():
        return True

    # English pages may link to a Chinese-only page by its generated site route.
    try:
        source_path = source.relative_to(ROOT / "docs" / "en").with_suffix("")
    except ValueError:
        return False

    # English is the default locale, so its generated pages live at the site root.
    page_route = source_path.parent if source.stem == "index" else source_path
    site_path = PurePosixPath(posixpath.normpath(str(page_route / raw_path)))
    if site_path.parts[:1] == ("zh",):
        site_path = PurePosixPath(*site_path.parts[1:])
    localized_path = ROOT / "docs" / "zh" / Path(*site_path.parts)
    return localized_path.with_suffix(".md").exists() or (localized_path / "index.md").exists()


def main() -> int:
    errors: list[str] = []
    for scan_root in SCAN_ROOTS:
        for source in markdown_files(scan_root):
            for raw_target in LINK_RE.findall(source.read_text(encoding="utf-8")):
                target = unquote(raw_target)
                parsed = urlparse(target)
                if parsed.scheme or parsed.netloc or target.startswith("#"):
                    continue
                path = (source.parent / parsed.path).resolve()
                try:
                    path.relative_to(ROOT)
                except ValueError:
                    errors.append(f"{source.relative_to(ROOT)}: link escapes repository: {raw_target}")
                    continue
                if not target_exists(source, path, parsed.path):
                    errors.append(f"{source.relative_to(ROOT)}: missing target: {raw_target}")

    if errors:
        print("Local documentation links failed:")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("Local documentation links: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
