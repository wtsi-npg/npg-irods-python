#!/bin/bash

set -ex

PYENV_RELEASE_VERSION=${PYENV_RELEASE_VERSION:="2.3.13"}
export PYENV_GIT_TAG="v${PYENV_RELEASE_VERSION}"

PYENV_ROOT=${PYENV_ROOT:-"$HOME/.pyenv"}
export PATH="$PYENV_ROOT/bin:$PATH"

curl -sSL -O https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer
/bin/bash ./pyenv-installer
