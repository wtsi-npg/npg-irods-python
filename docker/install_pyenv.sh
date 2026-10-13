#!/bin/bash

set -ex

PYENV_RELEASE_VERSION=${PYENV_RELEASE_VERSION:="2.6.16"}
export PYENV_GIT_TAG="v${PYENV_RELEASE_VERSION}"

PYENV_ROOT=${PYENV_ROOT:-"$HOME/.pyenv"}
export PATH="$PYENV_ROOT/bin:$PATH"

PYENV_SHA256="2bf587665219d2ed3e262fc2c3d9e6843b48f64435bc489d936f03e68b5f610d"
curl -sSL -O https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer
sha256sum ./pyenv-installer | grep "$PYENV_SHA256"
/bin/bash ./pyenv-installer
rm ./pyenv-installer
