##necessary libraries
import argparse
import zarr
import numpy as np
import json
import pandas as pd
import xarray as xr
from datetime import datetime
from netCDF4 import Dataset


def find_closest_index(array, target):
    """Finds the indices of the closest values to target in array, supporting datetime and numeric values."""
    if isinstance(array, (pd.Series, pd.Index)):
        array = array.to_numpy()
    
    array.sort()

    if isinstance(target, list):
        target = np.array(target)
    elif isinstance(target, (pd.Series, pd.Index)):
        target = target.to_numpy()

    if np.issubdtype(array.dtype, np.datetime64):
        array = array.astype('datetime64[s]').astype(np.float64)

    if np.issubdtype(target.dtype, np.datetime64):
        target = target.astype('datetime64[s]').astype(np.float64)

    closest_indices = np.argmin(np.abs(array[:, None] - target), axis=0)
    
    return closest_indices


def get_variable_attrs(vardic, igroup, invarname):
    """Fetch variable attributes based on `igroup`."""
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


def process_zarr_to_netcdf(indir, infile, outdir, igroup, invarname, z_values, freq='1h'):
    zin = zarr.open((indir + '/' + infile), "r")

    time = zin["datetime"][:]
    X = zin["X"][:]
    Y = zin["Y"][:]
    Z = zin["Z"][:]
    if invarname == "w":
        Z = zin["Z_edge"][:]
        
    with open((indir + '/' + infile + '/.zattrs')) as fp:
        vardic = json.load(fp)

    iunits, lname = get_variable_attrs(vardic, igroup, invarname)    
    
    ini_time = time[0]
    end_time = time[-1]

    varindx = zin.attrs[igroup + "_variable_index_map"][invarname]
    iz_range = find_closest_index(Z, np.array(z_values))
    time_steps = pd.date_range(start=ini_time, end=end_time, freq=freq)
    it_time = find_closest_index(time, time_steps)

    for i in it_time:
        time_value = time[i]
        time_str = str(time_value).replace(":", "-").replace(" ", "_").split(".")[0]
        outtime = f"test_{invarname}_{time_str}"
        outfile = outdir + f"/test_{invarname}_{outtime}.nc"

        # Check if the file already exists
        if os.path.exists(outfile):
            print(f"Skipping existing file: {outfile}")
            continue

        outvar = np.expand_dims(zin[igroup][int(i), varindx, :, :, iz_range], axis=0)

        ds_out = xr.Dataset(
            {invarname: (["time", "X", "Y", "Z"], outvar)},
            coords={
                "time": (["time"], np.array([time[i]], dtype='object')),
                "X": (["X"], X),
                "Y": (["Y"], Y),
                "Z": (["Z"], Z[iz_range])
            },
            attrs={"units": ("units", iunits), "long_name": ("long_name", lname)}
        )

        print("Writing: " + outfile)
        ds_out.to_netcdf(path=outfile, mode='w', format='NETCDF4', unlimited_dims='time')
        ds_out.close()
        del ds_out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Zarr to NetCDF.')
    parser.add_argument('--indir', type=str, required=True, help='Input directory containing the Zarr files.')
    parser.add_argument('--infile', type=str, required=True, help='Input Zarr filename.')
    parser.add_argument('--outdir', type=str, required=True, help='Output directory for NetCDF files.')
    parser.add_argument('--igroup', type=str, required=True, help='Group name in Zarr file.')
    parser.add_argument('--invarname', type=str, required=True, help='Variable name in Zarr file.')
    parser.add_argument('--z_values', type=float, nargs='+', required=True, help='Z values for the levels to extract.')
    parser.add_argument('--freq', type=str, default='1h', help='Frequency of the output time steps (e.g., 1h, 3h, 6h, 30min).')

    args = parser.parse_args()

    process_zarr_to_netcdf(args.indir, args.infile, args.outdir, args.igroup, args.invarname, args.z_values, args.freq)
