#!/usr/bin/env bash
# Prepares an isolated work directory for one coding-agent attempt and (optionally)
# launches the agent inside it.
#
# Usage:
#   ./run-agent.sh <attempt-name> [agent-command...]
#
# With no agent command, prints the pickup prompt and exits; the caller can open
# the work directory in their own terminal/agent UI.
#
# Examples:
#   ./run-agent.sh codex-gpt56-sol codex exec "pick up handoff.md and implement it; commit your work"
#   ./run-agent.sh claude-baseline claude --permission-mode default "pick up handoff.md and implement it; commit your work"
#
# Notes:
#   - The work dir is a clone of snapshot.bundle (HEAD = 30359e5), so the agent
#     never sees the reference solution that lives in the main repository's
#     history (commits 6dd474a..fbfe4f5).
#   - Needs JDK 11+ and Maven for `mvn test`.
set -euo pipefail

name="${1:?usage: run-agent.sh <attempt-name> [agent-command...]}"
shift

root="$(cd "$(dirname "$0")" && pwd)"
work="$root/work/$name"
if [ -e "$work" ]; then
    echo "work dir already exists, delete it first: $work" >&2
    exit 1
fi

git clone -q "$root/snapshot.bundle" "$work"
cd "$work"
git checkout -q -b attempt origin/arena-restore
cp "$root/handoff.md" .

if [ $# -eq 0 ]; then
    echo "workdir ready: $work"
    echo "pickup prompt: read handoff.md first, then implement requirements 1 and 2,"
    echo "run the test suite listed in handoff section 0, update docs/tests, and commit your work."
    exit 0
fi

exec "$@"
