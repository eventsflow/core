#!/bin/sh

set -e

echo "[INFO] Cleaning build directories" && \
	rm -rf *.egg-info

echo "[INFO] Cleaning cache files" && \
	find . -name "__pycache__" -type d # -exec rm -rf {} \;

echo "[INFO] Cleaning coverage files" && \
	rm -rf \
		.coverage.* \
		report.xml
