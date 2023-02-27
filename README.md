# Using kerchunk with Azure Blobs for SCHISM output

## Introduction

Zarr is an efficient format for indexing large sets of netCDF and HDF5 files. It allows for chunking, compression, and parallel I/O, which makes it an ideal choice for working with large scientific datasets. You can learn more about Zarr and its features on the Zarr documentation page (https://zarr.readthedocs.io/en/stable/).

Kerchunk is a tool that can create Zarr stores from existing netCDF and HDF5 files, making it easy to convert your existing datasets to the Zarr format. You can find the Kerchunk documentation and source code on GitHub (https://fsspec.github.io/kerchunk/).

fsspec is a library that provides a unified interface for working with various types of filesystems, including Azure and S3 blobs. This means that you can use fsspec to read and write data to and from these types of storage systems, which can be especially useful if you are working with large datasets that are stored in the cloud. You can learn more about fsspec on the fsspec documentation page (https://filesystem-spec.readthedocs.io/en/latest/).

Finally, Dask is a powerful tool for parallel computing in Python, and it includes built-in support for the Zarr format. This means that you can use Dask to read and process data stored in Zarr stores, and it will automatically chunk the data and distribute the processing across multiple cores or even multiple machines if necessary. You can learn more about Dask on the Dask documentation page (https://docs.dask.org/en/latest/), and specifically about using Zarr with Dask on the Dask-Zarr documentation page (https://docs.dask.org/en/latest/array-creation.html#zarr).

By leveraging all of these tools together, you can create a powerful workflow for working with large scientific datasets in Python, allowing you to slice through your information more efficiently and effectively.

## Notebooks

These are experimental notebooks to leverage kerchunk to generate Zarr indexes for existing netcdf files in Azure blobs

It takes a while to generate the needed jsons and combine them. These were done with dask for a subset of files and took about 30 minutes for ~ 3500 files and generated
a combined json of around 130 MB (could be compressed)

This way of access shows promise. See the screenshots for timings below :-
![opening data set](doc/images/loading_dataset_zarr.jpg)
![plotting a couple of slices](doc/images/demo_slicing_speeds.jpg)
