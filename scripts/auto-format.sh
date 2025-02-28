#! /usr/bin/env bash

set -x

ROOT_DIR=$(dirname "$(dirname "$0")")

cd "$ROOT_DIR/src" || exit 1

black .
isort .
autoflake --remove-all-unused-imports --in-place --recursive .
