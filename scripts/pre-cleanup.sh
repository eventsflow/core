#!/bin/sh

set -e

echo "[INFO] Cleaning build directories" && \
	rm -rf build dist

echo "[INFO] Cleaning files: *.pyc" && \
	find . -name "*.pyc" -delete

echo "[INFO] Cleaning cache files" && \
	find . -name "__pycache__" -type d # -exec rm -rf {} \;

echo "[INFO] Cleaning files: *.egg-info" && \
	rm -rf "*.egg-info"
