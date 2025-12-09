#!/bin/bash

# Run the docker container with volume mounting
# Mounts the entire workspace to /home/kleene/workspace to preserve paths

WORKSPACE_DIR="/home/kleene/workspace"

docker run -it --rm \
    -v "$WORKSPACE_DIR":"$WORKSPACE_DIR" \
    -w "$PWD" \
    spark-bigdata \
    bash run.sh
