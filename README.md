## Tools for post-processing PINACLES output

### Scripts
#### Customizable Selection of 3D Variables
- **`run_process_zarr.sh`**: A bash script to specify parameters of a given output variable. Information about the variable information (name, group) can be found at [PINACLES website](https://portal.nersc.gov/cfs/m1867/pinacles_docs/site/fields_zarr/).
- **`process_zarr.py`**: A Python script to extract a given variable at specific time intervals and vertical levels, and output each as a netcdf file.
