#!/usr/bin/env python3
"""Extract .figure blocks from the CHM blog HTMLs and screenshot each as PNG.

Usage: python3 extract_figures.py
Output: todo/blog/images/<slug>-fig<N>.png
"""
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
TODO = Path(__file__).resolve().parent.parent / "todo"
OUT = TODO / "blog" / "images"
WIDTH = 900  # CSS px; close to the article content width (980 - padding)
SCALE = 2    # device scale factor for retina-quality PNGs

FILES = {
    "lock": "chm-lock-mechanism.html",
    "iterator": "chm-iterator-no-duplicate.html",
    "fwd": "chm-forwarding-node.html",
    "sizectl": "chm-sizectl.html",
    "behaviors": "chm-counterintuitive-behaviors.html",
}

PAGE = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<style>
{css}
html, body {{ background: #fff; }}
body {{ padding: 0; }}
article {{ max-width: none; border: 0; border-radius: 0; padding: 12px; }}
</style></head>
<body><article>{figure}</article>
<script>document.title = String(document.body.scrollHeight + 24);</script>
</body></html>"""


def extract_figures(html: str):
    """Return list of full '<div class="figure">...</div>' source blocks."""
    blocks = []
    for m in re.finditer(r'<div class="figure">', html):
        depth, i = 1, m.start()  # the matched <div class="figure"> is already open
        while True:
            nxt_open = html.find("<div", i + 1)
            nxt_close = html.find("</div>", i + 1)
            if nxt_close == -1:
                raise ValueError("unbalanced <div> in figure block")
            if nxt_open != -1 and nxt_open < nxt_close:
                depth += 1
                i = nxt_open
            else:
                depth -= 1
                i = nxt_close
                if depth == 0:
                    blocks.append(html[m.start():nxt_close + len("</div>")])
                    break
    return blocks


def chrome_dom(args):
    """Run headless Chrome for --dump-dom; kill once '</html>' has been read.

    Chrome 151 on macOS renders fine but never exits on its own, so we
    terminate the process after the DOM has been fully written to stdout.
    """
    p = subprocess.Popen(
        [CHROME, "--headless=new", "--disable-gpu", "--no-first-run",
         "--hide-scrollbars", f"--user-data-dir={tempfile.mkdtemp()}"] + args,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    buf = []
    try:
        for line in p.stdout:
            buf.append(line)
            if "</html>" in line:
                break
    finally:
        p.kill()
        p.wait()
    return "".join(buf)


def chrome_shot(args, out: Path):
    """Run headless Chrome for --screenshot; kill once the PNG is written."""
    if out.exists():
        out.unlink()
    p = subprocess.Popen(
        [CHROME, "--headless=new", "--disable-gpu", "--no-first-run",
         "--hide-scrollbars", f"--user-data-dir={tempfile.mkdtemp()}"] + args,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(300):  # up to 30s
            time.sleep(0.1)
            if out.exists() and out.stat().st_size > 0:
                time.sleep(0.3)  # let the write finish
                return
        raise TimeoutError(f"screenshot timed out: {out}")
    finally:
        p.kill()
        p.wait()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp())
    total = 0
    for slug, fname in FILES.items():
        html = (TODO / fname).read_text(encoding="utf-8")
        css = re.search(r"<style>(.*?)</style>", html, re.S).group(1)
        figures = extract_figures(html)
        print(f"{fname}: {len(figures)} figures")
        for n, fig in enumerate(figures, 1):
            page = tmp / f"{slug}-{n}.html"
            page.write_text(PAGE.format(css=css, figure=fig), encoding="utf-8")
            # 1) measure height via --dump-dom (script writes it into <title>)
            dom = chrome_dom(["--virtual-time-budget=2000", "--dump-dom",
                              f"--window-size={WIDTH},1000",
                              f"file://{page}"])
            m = re.search(r"<title>(\d+)</title>", dom)
            if not m:
                print(f"  fig {n}: FAILED to measure height", file=sys.stderr)
                continue
            height = int(m.group(1))
            # 2) screenshot at measured size
            out = OUT / f"{slug}-fig{n}.png"
            try:
                chrome_shot([f"--screenshot={out}",
                             f"--window-size={WIDTH},{height}",
                             f"--force-device-scale-factor={SCALE}",
                             f"file://{page}"], out)
            except TimeoutError as e:
                print(f"  fig {n}: {e}", file=sys.stderr)
                continue
            ok = out.exists() and out.stat().st_size > 0
            print(f"  fig {n}: {out.name} {WIDTH}x{height}@{SCALE}x "
                  f"{'OK' if ok else 'FAILED'}")
            total += ok
    print(f"done: {total} images -> {OUT}")


if __name__ == "__main__":
    main()
