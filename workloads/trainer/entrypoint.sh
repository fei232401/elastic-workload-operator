#!/bin/sh
set -eu
host_flag=""
case "$(hostname)" in
  *-0) host_flag="--rdzv-conf=is_host=true,last_call_timeout=5" ;;
  *) host_flag="--rdzv-conf=last_call_timeout=5" ;;
esac
exec torchrun \
  --nnodes="${MIN_NODES}:${MAX_NODES}" \
  --nproc-per-node=1 \
  --max-restarts="${MAX_RESTARTS:-1000}" \
  --rdzv-backend=c10d \
  --rdzv-endpoint="${RDZV_ENDPOINT}" \
  --rdzv-id="${RDZV_ID}" \
  ${host_flag} \
  /app/train.py
