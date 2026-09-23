#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${LUPINE_SERVER:-}" ]]; then
  echo "warning: LUPINE_SERVER is not set; CUDA calls will not reach a LUPINE server" >&2
fi

cd "${COMFYUI_ROOT:-/app/ComfyUI}"
exec python main.py "$@"
