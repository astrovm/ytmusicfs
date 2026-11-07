#!/usr/bin/env bash
# Posts a weekly mutation report to the open "Missed mutants" issue, one or more comments, so a long
# list is never lost to GitHub's 65,536-character limit. Long reports are cut between lines; a cut
# inside a code fence closes the fence and opens it again in the next piece.
#
#   mutants-post.sh REPORT_FILE [HAS_FINDINGS]
#
# HAS_FINDINGS is "true" or "false" (default true): with no open issue, a clean report opens nothing.
# Needs GH_TOKEN and GH_REPO. MUTANTS_POST_LIMIT overrides the bytes per piece (for tests).
set -euo pipefail

report=${1:?usage: mutants-post.sh REPORT_FILE [HAS_FINDINGS]}
has_findings=${2:-true}
limit=${MUTANTS_POST_LIMIT:-60000}
pieces=$(mktemp -d)
trap 'rm -rf "$pieces"' EXIT

# Bytes, not characters: LC_ALL=C makes awk's length() count bytes. Room is kept for the fence and
# the "Continued" line each piece can gain.
LC_ALL=C awk -v dir="$pieces" -v limit="$limit" '
  function open_piece() {
    n++
    file = sprintf("%s/%04d", dir, n)
    size = 0
    if (fenced) { print "```" > file; size += 4 }
  }
  BEGIN { room = limit - 64; fenced = 0; n = 0; open_piece() }
  {
    if (size + length($0) + 1 > room && size > (fenced ? 4 : 0)) {
      if (fenced) print "```" > file
      close(file)
      open_piece()
    }
    print > file
    size += length($0) + 1
    if ($0 ~ /^```/) fenced = !fenced
  }
  END { close(file) }
' "$report"

mapfile -t files < <(find "$pieces" -type f | sort)
total=${#files[@]}
for i in "${!files[@]}"; do
  if [ "$i" -gt 0 ]; then
    { printf 'Continued, %d of %d.\n\n' "$((i + 1))" "$total"; cat "${files[$i]}"; } > "${files[$i]}.body"
    mv "${files[$i]}.body" "${files[$i]}"
  fi
done

issue=$(gh issue list --state open --search 'in:title "Missed mutants"' --json number --jq '.[0].number // empty')
if [ -z "$issue" ]; then
  [ "$has_findings" = true ] || exit 0
  url=$(gh issue create --title 'Missed mutants' --body-file "${files[0]}")
  issue=${url##*/}
  start=1
else
  start=0
fi
for ((i = start; i < total; i++)); do
  gh issue comment "$issue" --body-file "${files[$i]}" > /dev/null
done
echo "Posted $total piece(s) to issue $issue."
