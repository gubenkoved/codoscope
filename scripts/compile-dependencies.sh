#! /usr/bin/env bash

set -x

ROOT_DIR=$(dirname "$(dirname "$0")")

echo "Use --upgrade to update dependencies"

cd $ROOT_DIR

pip-compile "$@" -o requirements.txt
pip-compile "$@" -o dev-requirements.txt dev-requirements.in
