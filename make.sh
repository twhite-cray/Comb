#!/bin/bash
module load cray-mpich/8.1.16
module load rocm/5.0.2

export LD_LIBRARY_PATH="${CRAY_LD_LIBRARY_PATH}:${LD_LIBRARY_PATH}"
cd build_pinoak-5.0.2
make VERBOSE=1 -j
