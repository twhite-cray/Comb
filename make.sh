#!/bin/bash
module load rocm/5.0.2

cd build_pinoak-5.0.2
make VERBOSE=1 -j
