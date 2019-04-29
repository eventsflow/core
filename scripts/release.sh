#!/bin/bash

set -e

echo '[INFO] Cleanup'
scripts/pre-cleanup.sh

echo '[INFO] Release new version'
bumpversion part --tag
python3 setup.py sdist bdist_wheel
twine upload --username ownport dist/*
bumpversion --serialize {major}.{minor}.{patch}.dev0 patch
