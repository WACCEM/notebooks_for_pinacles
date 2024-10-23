#!/bin/bash

# Ensure Python environment is activated

# Directory containing multiple simulation subdirectories
PARENT_DIR="/pscratch/sd/w/wcmca1/PINACLES/rce/1km/run/"

# Parameters for processing
# IGROUP="ScalarState" # options are "ScalarState", "VelocityState" and "Diagnos"
IGROUP="DiagnosticState"
INVARNAME="thetav" # for more info about available variables visit https://portal.nersc.gov/cfs/m1867/pinacles_docs/site/fields_zarr/ 

# Output directory for NetCDF files
# OUTDIR="/pscratch/sd/p/paccini/temp/output_pinacles/${INVARNAME}"
OUTDIR="/pscratch/sd/f/feng045/PINACLES/rce/1km/fields3d/${INVARNAME}"

# Create the output directory if it does not exist
mkdir -p "$OUTDIR"

# Verify if the directory was created successfully and has write permissions
if [ ! -d "$OUTDIR" ]; then
    echo "Failed to create output directory: $OUTDIR"
    exit 1
elif [ ! -w "$OUTDIR" ]; then
    echo "Output directory is not writable: $OUTDIR"
    exit 1
else
    echo "Output directory is ready: $OUTDIR"
fi

## Set parallel options
run_parallel=1  # 0: serial, 1: parallel
n_workers=32  # number of workers (CPU in a node)

## Optional input parameters
# Vertical levels
Z_VALUES=(100 500 1000 1500 3000 4500 5500 7500 9500 10500 12000 )
# Temporal frequency: '1h', '3h', '6h', '30min',etc
FREQ="10min"
# Time period (format: 'yyyy-mo-dyThh:mm:ss')
start_time='2000-01-07T00'
end_time='2000-01-09T23'

# Loop over each subdirectory in the parent directory
for SUBDIR in "$PARENT_DIR"/test_1km_01_started_*/; do
    
    INFILE="fields.zarr"
    
    echo "Starting processing for directory: $SUBDIR"
  
    python process_zarr.py --indir "$SUBDIR" --infile "$INFILE" --outdir "$OUTDIR" --igroup "$IGROUP" --invarname "$INVARNAME" \
        --z_values "${Z_VALUES[@]}" --freq ${FREQ} \
        --s_time ${start_time} --e_time ${end_time} \
        --parallel ${run_parallel} --n_workers ${n_workers}
    
    
    if [ $? -eq 0 ]; then
        echo "Finished processing for directory: $SUBDIR"
    else
        echo "Error encountered while processing directory: $SUBDIR"
    fi
    
    
done

# Final completion message
echo "All directories processed."
