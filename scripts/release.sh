#!/bin/bash

set -e

bumpversion part --tag
twine upload --username ownport dist/*
bumpversion --serialize {major}.{minor}.{patch}.dev0 patch
