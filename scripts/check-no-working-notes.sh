#!/usr/bin/env bash
# Refuse working notes in the repository: plans, specs, and instructions for
# coding agents. They belong in the tracker or in a local file, not in history.
#
#   scripts/check-no-working-notes.sh            # the tracked tree
#   scripts/check-no-working-notes.sh <base-ref> # every commit since base-ref
set -euo pipefail

pattern='(^|/)(CLAUDE|AGENTS|GEMINI|COPILOT|IMPLEMENTATION-NOTES|PLAN|PLANS|TODO|NOTES)(\.[a-z]+)?\.md$'
pattern+='|(^|/)\.(claude|codex|cursor|aider[^/]*|windsurf)(/|$)'
pattern+='|(^|/)docs/(plans|specs|superpowers)/'
pattern+='|(^|/)\.github/copilot-instructions\.md$'

if [ $# -eq 0 ]; then
  found=$(git ls-files | grep -Ei "$pattern" || true)
else
  found=$(git log --format= --name-only --diff-filter=A "$1..HEAD" | sort -u | grep -Ei "$pattern" || true)
fi

if [ -n "$found" ]; then
  echo "Working notes do not belong in this repository:" >&2
  printf '  %s\n' $found >&2
  echo "Keep plans and agent instructions outside the repository (see CONTRIBUTING.md)." >&2
  exit 1
fi
