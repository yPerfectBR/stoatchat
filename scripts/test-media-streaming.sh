#!/usr/bin/env bash
set -euo pipefail

url="${1:-${STOAT_MEDIA_TEST_URL:-}}"
if [[ -z "$url" ]]; then
  echo "uso: $0 <URL-direta-de-video>" >&2
  echo "ou defina STOAT_MEDIA_TEST_URL" >&2
  exit 2
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

effective="$(curl -fsSL -o /dev/null -w '%{url_effective}' "$url")"
echo "URL: $effective"

curl -fsSI "$effective" > "$tmp/head"
grep -qi '^accept-ranges:[[:space:]]*bytes' "$tmp/head"
size="$(awk 'BEGIN{IGNORECASE=1} /^content-length:/ {gsub("\\r", "", $2); print $2}' "$tmp/head" | tail -1)"
[[ "$size" =~ ^[0-9]+$ ]] || { echo 'Content-Length ausente/inválido no HEAD'; exit 1; }

status="$(curl -sS -D "$tmp/range.headers" -o "$tmp/range.bin" -w '%{http_code}' -H 'Range: bytes=1024-2047' "$effective")"
[[ "$status" == 206 ]]
grep -qi '^accept-ranges:[[:space:]]*bytes' "$tmp/range.headers"
grep -qiE '^content-range:[[:space:]]*bytes 1024-2047/[0-9]+' "$tmp/range.headers"
[[ "$(wc -c < "$tmp/range.bin")" -eq 1024 ]]

status="$(curl -sS -D "$tmp/suffix.headers" -o "$tmp/suffix.bin" -w '%{http_code}' -H 'Range: bytes=-1024' "$effective")"
[[ "$status" == 206 ]]
[[ "$(wc -c < "$tmp/suffix.bin")" -eq 1024 ]]

invalid_start=$((size + 1024))
status="$(curl -sS -D "$tmp/invalid.headers" -o /dev/null -w '%{http_code}' -H "Range: bytes=${invalid_start}-$((invalid_start + 100))" "$effective")"
[[ "$status" == 416 ]]
grep -qi "^content-range:[[:space:]]*bytes \*/${size}" "$tmp/invalid.headers"

echo 'PASS: HEAD / 206 / Content-Range / suffix / 416'
