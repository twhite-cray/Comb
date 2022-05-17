#!/bin/bash
module load rocm/5.0.2
set -x
export MPICH_GPU_SUPPORT_ENABLED=1
rm -rf Comb_*
srun -l -u -t 5:00 -p bardpeak -N 1 -n 8 --cpu-bind=mask_cpu:0xff000000000000,0xff00000000000000,0xff0000,0xff000000,0xff,0xff00,0xff00000000,0xff0000000000 bash -c 'HIP_VISIBLE_DEVICES=${SLURM_LOCALID} ./comb 400_400_400 -divide 2_2_2 -periodic 1_1_1 -ghost 1_1_1 -vars 3 -cycles 25 -comm cutoff 250 -exec disable seq -exec enable hip -hip_aware_mpi -use_device_for_hip_util_aloc -memory enable hip_device -comm disable mock -comm enable mpi -comm enable mpi_active_rma'

