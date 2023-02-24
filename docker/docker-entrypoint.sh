#!/bin/bash

set -e

export PYENV_ROOT="/app/.pyenv"
export PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"
export PYTHONUNBUFFERED=1

exec "$@"
