#!/usr/bin/env bash
# Deletes a per-run warp benchmark bucket (when BUCKET is set) and sweeps leaked
# tag-warp-ci-* buckets older than MAX_AGE_HOURS. Benchmark CI runs this after every run;
# it is also runnable locally (with AWS credentials in the environment) to reproduce or
# debug the sweep — the logic deliberately does NOT live inline in workflow YAML.
#
# Env:
#   BUCKET         per-run bucket to delete first (optional)
#   ENDPOINT       S3 endpoint            (default https://t3.storage.dev)
#   MAX_AGE_HOURS  sweep age threshold    (default 24)
#
# Best-effort by design: individual delete failures are ignored (another run may have
# already removed a bucket), but an unparseable CreationDate is logged loudly instead of
# silently skipped — a format/locale drift would otherwise turn the sweep into a permanent
# no-op while leaked buckets accumulate.
set -u

ENDPOINT="${ENDPOINT:-https://t3.storage.dev}"
MAX_AGE_HOURS="${MAX_AGE_HOURS:-24}"

if [ -n "${BUCKET:-}" ]; then
  aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET" --recursive --only-show-errors || true
  aws --endpoint-url "$ENDPOINT" s3api delete-bucket --bucket "$BUCKET" || true
fi

cutoff=$(($(date -u +%s) - MAX_AGE_HOURS * 3600))
aws --endpoint-url "$ENDPOINT" s3api list-buckets \
  --query 'Buckets[?starts_with(Name, `tag-warp-ci-`)].[Name,CreationDate]' --output text |
  while read -r name created; do
    [ -n "$name" ] && [ "$name" != "None" ] || continue
    if ! ts=$(date -u -d "$created" +%s 2>/dev/null); then
      echo "WARNING: cannot parse CreationDate '$created' for bucket $name - skipping" >&2
      continue
    fi
    [ "$ts" -lt "$cutoff" ] || continue
    echo "Sweeping leaked benchmark bucket: $name (created $created)"
    aws --endpoint-url "$ENDPOINT" s3 rm "s3://$name" --recursive --only-show-errors || true
    aws --endpoint-url "$ENDPOINT" s3api delete-bucket --bucket "$name" || true
  done || true

exit 0
