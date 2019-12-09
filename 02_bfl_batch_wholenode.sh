#!/bin/bash --login
###
# project code
#SBATCH --account=scw1433
#job name
#SBATCH --job-name=bflNode
#job stdout file
#SBATCH --output=bflNode.out.%J
#job stderr file
#SBATCH --error=bflNode.err.%J
#maximum job time in D-HH:MM
#SBATCH --time=0-04:00
#number of parallel processes (tasks) you are requesting - maps to MPI processes
#SBATCH --ntasks=40
#memory per process in MB 
#SBATCH --ntasks-per-node=40
#SBATCH --mem=360G
#tasks to run per node (change for hybrid OpenMP/MPI) 
###

#now run normal batch commands 
module load R/3.5.3 compiler/intel/2018/2 udunits gdal proj/4.9.3 szip
export CXX='icpc -std=c++11' UDUNITS2_LIBS=/apps/libraries/udunits/2.2.26/el7/AVX512/intel-2018/lib R_HOME=/apps/languages/R/3.5.3/el7/AVX512/intel-2018/lib64/R R_LIBS_USER=/scratch/s.j.a.h.jones/R_sf

# time Rscript bfl_sunbird_byseg.R CO_basin_rivers.shp CO_barriers_ED50_sersnapped.shp 2192 $SLURM_NTASKS
# time Rscript bfl_sunbird_byseg.R FR_basin_rivers.shp FR_barriers_snap.shp 2192 $SLURM_NTASKS
# time Rscript bfl_sunbird_byseg_50percent.R FR_basin_rivers.shp FR_barriers_snap.shp 2192 $SLURM_NTASKS
time Rscript bfl_sunbird_byseg_clean.R FR_basin_rivers.shp FR_barriers_snap.shp 2192 $SLURM_NTASKS