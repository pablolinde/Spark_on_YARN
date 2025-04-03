#!/bin/bash
# install_deps.sh

NODES=("namenode" "datanode1" "datanode2" "datanode3" "datanode4")
PYTHON_DEPS=("reverse_geocoder" "pycountry")

for node in "${NODES[@]}"; do
    echo "Installing on $node..."
    docker exec -it $node bash -c "\
        apt-get update && \
        apt-get install -y python3-pip && \
        pip install ${PYTHON_DEPS[*]}"
        done