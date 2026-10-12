#!/bin/bash

# Run the docker container with volume mounting
# Mounts the selected workspace at the same container path

WORKSPACE_DIR="${WORKSPACE_DIR:-$PWD}"

docker run -it --rm \
    -v "$WORKSPACE_DIR":"$WORKSPACE_DIR" \
    -w "$PWD" \
    spark-bigdata \
    bash run.sh
