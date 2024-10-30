##necessary libraries
import argparse
import os, time
import zarr
import numpy as np
import json
import pandas as pd
import xarray as xr
import dask
from dask.distributed import Client, LocalCluster


def find_closest_index(array, target):
    """
    Finds the indices of the closest values to target in array, supporting datetime and numeric values.
    For datetime values, it ensures exact match.
    """
    if isinstance(array, (pd.Series, pd.Index)):
        array = array.to_numpy()
       
    if isinstance(target, list):
        target = np.array(target)
    elif isinstance(target, (pd.Series, pd.Index)):
        target = target.to_numpy()
        
    if np.issubdtype(array.dtype, np.datetime64):
        array = array.astype('datetime64[s]')
        target = target.astype('datetime64[s]')
        # Handle exact matches for datetime
        exact_indices = np.where(np.isin(array, target))[0]
        return exact_indices
    else:
        if np.issubdtype(target.dtype, np.datetime64):
            target = target.astype('datetime64[s]').astype(np.float64)
        if np.issubdtype(array.dtype, np.datetime64):
            array = array.astype('datetime64[s]').astype(np.float64)

        # Handle closest matches for other numeric types
        closest_indices = np.argmin(np.abs(array[:, None] - target), axis=0)
        return closest_indices


def get_variable_attrs(vardic, igroup, invarname):
    """
    Fetch variable attributes based on `igroup`.
    """
    if igroup == 'ScalarState':
        iunits = vardic['ScalarState_units'][invarname]
        lname = vardic['ScalarState_long_names'][invarname]
    elif igroup == 'DiagnosticState':
        iunits = vardic['DiagnosticState_units'][invarname]
        lname = vardic['DiagnosticState_long_names'][invarname]
    elif igroup == 'VelocityState':
        iunits = vardic['VelocityState_units'][invarname]
        lname = vardic['VelocityState_long_names'][invarname]
    else:
        raise ValueError(f"Unknown igroup: {igroup}")
    
    return iunits, lname

def extract_3dvar(data_dict):
    """
    Extract a 3D variable from zarr and write to a netCDF file.

    Args:
        data_dict: dictionary
            A dictionary containing all inputs.
    
    Returns:
        outfile: string
            Output filename.
    """
    # Get variables from dictionary
    tt = data_dict['tt']
    time_in = data_dict['time_in']
    zin = data_dict['zin']
    outdir = data_dict['outdir']
    invarname = data_dict['data_dict']
    igroup = data_dict['igroup']
    varindx = data_dict['varindx']
    z_values = data_dict['z_values']
    iunits = data_dict['iunits']
    lname = data_dict['lname']
    ref_time = data_dict['ref_time']

    # Get coordinate variables
    X = zin["X"][:]
    Y = zin["Y"][:]
    Z = zin["Z"][:]
    if invarname == "w":
        Z = zin["Z_edge"][:]

    # Find closest Z indices from input
    if z_values is not None:
        iz_range = find_closest_index(Z, np.array(z_values))
    else:
        # Make full Z indices
        iz_range = np.arange(0, len(Z), 1)

    # Create output filename
    time_value = time_in
    time_str = str(time_value).replace(":", "-").replace(" ", "_").split(".")[0]
    outtime = f"{time_str}"
    outfile = f"{outdir}/{invarname}_{outtime}.nc"

    # # Check if the file already exists
    # if os.path.exists(outfile):
    #     print(f"Skipping existing file: {outfile}")
    #     continue

    # Get 3D variable
    outvar = np.expand_dims(zin[igroup][int(tt), varindx, :, :, iz_range], axis=0)
    # Variable attributes
    outvar_attrs = {
        "long_name": lname,
        "units": iunits, 
    }

    # Make output Xarray DataSet
    var_dict = {
        invarname: (["time", "X", "Y", "Z"], outvar, outvar_attrs),
    }
    coord_dict = {
        "time": (["time"], np.array([time_in], dtype='object')),
        "X": (["X"], X),
        "Y": (["Y"], Y),
        "Z": (["Z"], Z[iz_range])
    }
    gattr_dict = {
        'created_on': time.ctime(time.time()),
    }
    ds_out = xr.Dataset(var_dict, coords=coord_dict, attrs=gattr_dict)
    # Change the encoding for the time variable
    ds_out["time"].encoding["units"] = f"seconds since {ref_time}"
    ds_out["time"].time.encoding["calendar"] = "standard"

    # print(f"Writing: {outfile}")
    ds_out.to_netcdf(path=outfile, mode='w', format='NETCDF4', unlimited_dims='time')
    ds_out.close()
    print(f"Saved: {outfile}")
    del ds_out
    return outfile


def process_zarr_to_netcdf(
        indir, infile, outdir, igroup, invarname, 
        z_values=None, freq=None,
        start_time=None, end_time=None, ref_time=None,
        run_parallel=0, n_workers=1,
):
    """
    Extract a variable from zarr and write to netCDF files.

    Args:
        indir: string
            Zarr data input directory.
        infile: string
            Zarr data input filename.
        outdir: string
            Output netCDF file directory.
        igroup: string
            Name of the group for the variable.
        invarname: string
            Desired variable name.
        z_values: array-like, optional, default=None
            Vertical levels to extract the variable.
        freq: string, optional, default=None
            Temporal frequency to extract the variable.
        start_time: string, optional, default=None
            Start datetime to process ('yyyy-mo-dyThh:mm:ss').
        end_time: string, optional, default=None
            End datetime to process ('yyyy-mo-dyThh:mm:ss').
        ref_time: string, optional, default=None
            Reference datetime ('yyyy-mo-dyThh:mm:ss')
        run_parallel: int, optional, default=0
            Flag to run processing in parallel.
        n_workers: int, optional, default=1
            Number of workers to run in parallel.

    Returns:
        None.
    """
    # Open zarr file
    zin = zarr.open((indir + '/' + infile), "r")
    # Get coordinate variables
    time_in = zin["datetime"][:]
    
    # Read zarr file attributes
    with open((indir + '/' + infile + '/.zattrs')) as fp:
        vardic = json.load(fp)

    # Get variable attributes
    iunits, lname = get_variable_attrs(vardic, igroup, invarname)    
    
    # Get variable index from attribute
    varindx = zin.attrs[igroup + "_variable_index_map"][invarname]

    # # If start_time and end_time are provided, convert to Pandas Datetime
    # if (start_time is not None) & (end_time is not None):
    #     _start_time = pd.to_datetime(start_time)
    #     _end_time = pd.to_datetime(end_time)
    #     _time_in = pd.DatetimeIndex(time_in)
    
    # # Find closest time indices from input considering frequency and range
    # if freq is not None:
    #     if (start_time is not None) & (end_time is not None):
    #         time_steps = pd.date_range(start=_start_time, end=_end_time, freq=freq)
    #         it_time = find_closest_index(_time_in, time_steps)
    #     else:
    #         time0 = time_in[0]
    #         time1 = time_in[-1]
    #         time_steps = pd.date_range(start=time0, end=time1, freq=freq)
    #         it_time = find_closest_index(time_in, time_steps)
    # else:
    #     # Make full time indices
    #     if (start_time is not None) & (end_time is not None):
    #         it_time = np.where((_time_in >= _start_time) & (_time_in <= _end_time))[0]
    #     else:
    #         it_time = np.arange(len(time_in))


    # Find closest time indices from input
    if freq is not None:
        time0 = time_in[0]
        time1 = time_in[-1]
        time_steps = pd.date_range(start=time0, end=time1, freq=freq)
        it_time = find_closest_index(time_in, time_steps)
    else:
        # Make full time indices
        it_time = np.arange(0, len(time_in), 1)

    # Find times within input range
    if (start_time is not None) & (end_time is not None):
        # Convert to Pandas Datetime
        _start_time = pd.to_datetime(start_time)
        _end_time = pd.to_datetime(end_time)
        # Subset times after frequency is matched
        time_freq = time_in[it_time]
        # Convert to Pandas DatetimeIndex
        _time_in = pd.DatetimeIndex(time_freq)
        # Subset times within the input range
        time_out = _time_in[(_time_in >= _start_time) & (_time_in <= _end_time)]
        # Find indices in time_in that match time_out
        it_time = pd.DatetimeIndex(time_in).get_indexer(time_out)

    # Check number of times
    if len(it_time) > 0:

        # Initialize dask for parallel
        if run_parallel == 1:
            cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
            client = Client(cluster)

        results = []

        # Iterate over time
        for tt in it_time:
            # Put variables in a dictionary
            data_dict = {}
            data_dict['tt'] = tt
            data_dict['time_in'] = time_in[tt]
            data_dict['zin'] = zin
            data_dict['outdir'] = outdir
            data_dict['data_dict'] = invarname
            data_dict['igroup'] = igroup
            data_dict['varindx'] = varindx
            data_dict['z_values'] = z_values
            data_dict['iunits'] = iunits
            data_dict['lname'] = lname
            data_dict['ref_time'] = ref_time
            # Call extract function
            if run_parallel == 0:
                result = extract_3dvar(data_dict)
            elif run_parallel == 1:
                result = dask.delayed(extract_3dvar)(data_dict)
            results.append(result)

        # Execute parallel computation
        if run_parallel == 1:
            final_result = dask.compute(*results)

    else:
        print(f"No times found between {start_time} and {end_time}")



if __name__ == "__main__":
    # Define and retrieve the command-line arguments...
    parser = argparse.ArgumentParser(description='Process Zarr to NetCDF.')
    parser.add_argument('--indir', type=str, required=True, help='Input directory containing the Zarr files.')
    parser.add_argument('--infile', type=str, required=True, help='Input Zarr filename.')
    parser.add_argument('--outdir', type=str, required=True, help='Output directory for NetCDF files.')
    parser.add_argument('--igroup', type=str, required=True, help='Group name in Zarr file.')
    parser.add_argument('--invarname', type=str, required=True, help='Variable name in Zarr file.')
    parser.add_argument('--z_values', type=float, nargs='+', default=None, help='Z values for the levels to extract.')
    parser.add_argument('--freq', type=str, default=None, help='Frequency of the output time steps (e.g., 1h, 3h, 6h, 30min).')
    parser.add_argument('--s_time', type=str, default=None, help='Start datetime to process (yyyy-mo-dyThh:mm:ss).')
    parser.add_argument('--e_time', type=str, default=None, help='End datetime to process (yyyy-mo-dyThh:mm:ss).')
    parser.add_argument('--ref_time', type=str, default='2000-01-01T00:00:00', help='Reference datetime (yyyy-mo-dyThh:mm:ss).')
    parser.add_argument("--parallel", help="Flag to run in parallel (0:serial, 1:parallel)", type=int, default=0)
    parser.add_argument("--n_workers", help="Number of workers to run in parallel", type=int, default=1)
    args = parser.parse_args()

    process_zarr_to_netcdf(args.indir, args.infile, args.outdir, args.igroup, args.invarname, 
                           z_values=args.z_values, freq=args.freq,
                           start_time=args.s_time, end_time=args.e_time, ref_time=args.ref_time,
                           run_parallel=args.parallel, n_workers=args.n_workers)
