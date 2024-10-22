#! /usr/bin/env bash

black .
isort .
autoflake --remove-all-unused-imports --in-place --recursive .
