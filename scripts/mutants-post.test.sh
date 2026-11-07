#!/usr/bin/env bash
# Tests scripts/mutants-post.sh against a fake gh that records what would be posted.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin"
cat > "$work/bin/gh" <<'FAKE'
#!/usr/bin/env bash
case "$1 $2" in
  "issue list") [ -f "$FAKE/open" ] && cat "$FAKE/open"; exit 0 ;;
  "issue create") while [ $# -gt 0 ]; do [ "$1" = --body-file ] && cp "$2" "$FAKE/post-$(ls "$FAKE" | grep -c post-)"; shift; done; echo "https://github.com/o/r/issues/77"; touch "$FAKE/created" ;;
  "issue comment") n=$3; while [ $# -gt 0 ]; do [ "$1" = --body-file ] && cp "$2" "$FAKE/post-$(ls "$FAKE" | grep -c post-)"; shift; done; echo "$n" >> "$FAKE/commented" ;;
  *) echo "unexpected: $*" >&2; exit 1 ;;
esac
FAKE
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH"
fail() { echo "FAIL: $1" >&2; exit 1; }
run_case() { export FAKE="$work/$1"; mkdir -p "$FAKE"; }

# A long report with no open issue opens one, then comments; every row survives, and each piece fits.
run_case long
{ echo "Parts 1: run"; echo; echo '### Lived (5000)'; echo '```'; for i in $(seq 1 5000); do echo "src/a.ts:$i  survived: StringLiteral → \"x\""; done; echo '```'; } > "$work/report.md"
"$here/mutants-post.sh" "$work/report.md" true > /dev/null
[ -f "$FAKE/created" ] || fail "no issue created"
[ "$(cat "$FAKE"/post-* | grep -c '^src/a.ts:')" = 5000 ] || fail "rows lost"
pieces=$(find "$FAKE" -name 'post-*' | wc -l)
[ "$pieces" -gt 1 ] || fail "not split"
grep -q 77 "$FAKE/commented" || fail "comments not on the new issue"
for f in "$FAKE"/post-*; do
  [ "$(wc -c < "$f")" -le 60000 ] || fail "piece over the limit"
  [ $(( $(grep -c '^```' "$f") % 2 )) = 0 ] || fail "unbalanced fence"
done
grep -q "Continued, 2 of $pieces" "$FAKE/post-1" || fail "no continuation line"

# A short report on an open issue is one comment.
run_case short; echo '[]' >/dev/null; echo 12 > "$FAKE/open"
printf 'Parts 1\n\n### Lived (1)\n```\nsrc/a.ts:1 x\n```\n' > "$work/report.md"
"$here/mutants-post.sh" "$work/report.md" true > /dev/null
[ "$(cat "$FAKE/commented")" = 12 ] || fail "not one comment on 12"
[ ! -f "$FAKE/created" ] || fail "opened a second issue"

# A clean report with no open issue opens nothing.
run_case clean
printf 'Parts 1: every change was caught.\n' > "$work/report.md"
"$here/mutants-post.sh" "$work/report.md" false > /dev/null
[ ! -f "$FAKE/created" ] || fail "opened an issue for a clean report"

# A row of multi-byte text counts in bytes, not characters.
run_case bytes
{ echo '```'; for i in $(seq 1 3000); do printf 'ñññññññññññññññññññññññññññññññññññññññ%d\n' "$i"; done; echo '```'; } > "$work/report.md"
"$here/mutants-post.sh" "$work/report.md" true > /dev/null
for f in "$FAKE"/post-*; do [ "$(wc -c < "$f")" -le 60000 ] || fail "bytes over the limit"; done
echo "mutants-post: all passed"
