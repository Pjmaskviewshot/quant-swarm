#!/usr/bin/env bash
# Build APEX_FINAL/ and the distributable archive.
#
# Deliberately explicit about what it EXCLUDES: no .env, no .git, no database,
# no persisted model state, no __pycache__. The archive is scanned for secrets
# before the checksums are written, and the build aborts if anything is found.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

COMMIT="$(git rev-parse --short HEAD)"
OUT="${1:-$REPO/APEX_FINAL}"
ARCHIVE="${2:-$REPO/APEX_FINAL_${COMMIT}.zip}"

rm -rf "$OUT" "$ARCHIVE"
mkdir -p "$OUT"

echo "==> assembling $OUT (commit $COMMIT)"
for d in src tests scripts docs reports .github; do
    [ -d "$d" ] && cp -r "$d" "$OUT/"
done
for f in README.md CHANGELOG.md requirements.txt requirements-dev.txt \
         requirements.lock pytest.ini .env.example .gitignore \
         FINAL_APEX_REPORT.md; do
    [ -f "$f" ] && cp "$f" "$OUT/"
done

# Strip anything that must never ship.
find "$OUT" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
find "$OUT" \( -name '*.pyc' -o -name '.env' -o -name '*.db' -o -name '*.db-*' \
            -o -name 'sgd_state.json' -o -name '.DS_Store' \) -delete 2>/dev/null || true
rm -rf "$OUT/.pytest_cache" "$OUT/data" "$OUT/reports/experiments" 2>/dev/null || true

echo "==> secret scan"
HITS=$(grep -rEil "(api[_-]?key|api[_-]?secret|password|private[_-]?key|bearer)[[:space:]]*[:=][[:space:]]*[\"'][A-Za-z0-9_/+=-]{12,}[\"']" "$OUT" 2>/dev/null || true)
if [ -n "$HITS" ]; then
    echo "ABORT: possible secrets in the package:"; echo "$HITS"; exit 1
fi
ENTROPY=$(grep -rEl "[\"'][A-Za-z0-9]{40,}[\"']" "$OUT" --include='*.py' --include='*.json' --include='*.yml' 2>/dev/null || true)
[ -n "$ENTROPY" ] && { echo "REVIEW: long literals found in:"; echo "$ENTROPY"; }
echo "    clean"

echo "==> manifest"
( cd "$OUT" && find . -type f | sort | while read -r f; do
    printf '%s  %s\n' "$(sha256sum "$f" | cut -d' ' -f1)" "${f#./}"
  done ) > "$OUT/MANIFEST.sha256"
echo "    $(wc -l < "$OUT/MANIFEST.sha256") files"

echo "==> archive"
( cd "$(dirname "$OUT")" && zip -qr "$ARCHIVE" "$(basename "$OUT")" -x '*__pycache__*' )
sha256sum "$ARCHIVE" > "${ARCHIVE}.sha256"

echo
echo "commit:   $COMMIT"
echo "package:  $OUT"
echo "archive:  $ARCHIVE"
echo "sha256:   $(cut -d' ' -f1 < "${ARCHIVE}.sha256")"
echo "size:     $(du -h "$ARCHIVE" | cut -f1)"
