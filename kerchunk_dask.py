import os
import time
import functools

import fsspec
import ujson
import xarray as xr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from tqdm import tqdm
import adlfs
print(adlfs.__version__)

def glob_urls(url_pattern, account_dict):
    # Initiate fsspec filesystems for reading and writing
    fs_read = fsspec.filesystem("abfs", **account_dict)
    paths = fs_read.glob(url_pattern)
    return ['abfs://'+p for p in paths]

# Define a decorator to retry a function if it fails
def retry_on_failure(max_retries=3, delay=1):
    def decorator_retry_on_failure(func):
        @functools.wraps(func)
        def wrapper_retry_on_failure(*args, **kwargs):
            num_retries = 0
            while num_retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except:
                    num_retries += 1
                    time.sleep(delay)
            raise Exception("Function failed after %d retries" % max_retries)
        return wrapper_retry_on_failure
    return decorator_retry_on_failure

# Use Kerchunk's `SingleHdf5ToZarr` method to create a `Kerchunk` index from a NetCDF file.
@retry_on_failure()
def generate_json_reference(fs_read, u, output_dir, so_dict):
    with fs_read.open(u, **so_dict) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        fname = u.split("/")[-1].strip(".nc")
        outf = f"{output_dir}/{fname}.json"
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
        return outf


from kerchunk.combine import MultiZarrToZarr
import dask
import logging
from distributed import Client

#client = Client(n_workers=8, silence_logs=logging.ERROR)
#client

def create_combined(urls,account_dict,output_dir, coo_map, concat_dims, identical_dims, json_file):
    """
    Create a combined zarr store from a list of urls.

    Parameters
    ----------
    urls : list
        List of urls to combine
    account_dict : dict
        Dictionary containing Azure account credentials
    output_dir : str
        Directory to store json files
    coo_map : dict
        Dictionary mapping coordinate names to the dimension they correspond to
    concat_dims : list
        List of dimensions that are concatenated
    identical_dims : list
        List of dimensions that are identical
    json_file : str
        Path to store the json file
    """
    # Initiate fsspec filesystems for reading and writing
    fs_read = fsspec.filesystem("abfs", **account_dict)
    so_dict = dict(default_fill_cache=False, default_cache_type="first")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    tasks = [dask.delayed(generate_json_reference)(fs_read, u, output_dir, so_dict) for u in urls]
    dask.compute(tasks)
    json_files = [f"{output_dir}/{u.split('/')[-1].split('.')[0]}.json" for u in urls]
    zz = MultiZarrToZarr(json_files,
                         remote_protocol='abfs',remote_options=account_dict,
                         coo_map=coo_map,
                         concat_dims=concat_dims, identical_dims=identical_dims)
    with open(json_file,'wb') as ofh:
        ofh.write(ujson.dumps(zz.translate()).encode())
