#!/usr/bin/env bash
# Keep one comment on the pull request listing the changes no test caught (what
# scripts/mutants.py wrote to --out DIR), and delete it once every one is caught.
# Usage: scripts/mutants-comment.sh DIR
# Needs GH_TOKEN, GITHUB_REPOSITORY, PR and RUN; FORK=true only summarizes.
set -euo pipefail
results=${1:?Usage: scripts/mutants-comment.sh DIR}
marker='<!-- mutants -->'
lived=$(cat "$results/lived.txt" 2>/dev/null || true)
tally=$(cat "$results/tally.txt" 2>/dev/null || echo "0 of 0")
body="$marker
**Mutation testing:** $tally changes caught by a test.

\`\`\`
$lived
\`\`\`

Each one is a change to the code no test noticed. Not a blocker: real gaps get their own PR. [Full report]($RUN)"
# Keep the body within GitHub's limit, cutting only between complete rows.
if (( ${#body} > 60000 )) || (( $(printf '%s' "$body" | wc -c) > 60000 )); then
  prefix="$marker
**Mutation testing:** $tally changes caught by a test.

\`\`\`"
  suffix="\`\`\`

Report truncated. [Full report]($RUN)"
  budget=$((60000 - $(printf '%s\n%s\n' "$prefix" "$suffix" | wc -c)))
  rows=$(printf '%s\n' "$lived" | LC_ALL=C awk -v limit="$budget" '{ if (size + length($0) + 1 > limit) full = 1; if (!full) { print; size += length($0) + 1 } }')
  body="$prefix
$rows
$suffix"
fi
[[ -z $lived ]] || printf '%s\n' "$body" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
[[ ${FORK:-false} != true ]] || exit 0
old=$(gh api --paginate "repos/$GITHUB_REPOSITORY/issues/$PR/comments" \
  --jq ".[] | select(.user.login == \"github-actions[bot]\" and (.body | startswith(\"$marker\"))) | .id" | head -n 1)
if [[ -z $lived ]]; then
  if [[ -n $old ]]; then gh api -X DELETE "repos/$GITHUB_REPOSITORY/issues/comments/$old"; fi
elif [[ -n $old ]]; then
  gh api -X PATCH "repos/$GITHUB_REPOSITORY/issues/comments/$old" -f body="$body" >/dev/null
else
  gh api "repos/$GITHUB_REPOSITORY/issues/$PR/comments" -f body="$body" >/dev/null
fi
