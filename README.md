# Dataflux for Google Cloud Storage Python client library

## Overview

This is the client library backing the [Dataflux Dataset for Pytorch](https://github.com/GoogleCloudPlatform/dataflux-pytorch). The purpose of this client is to quickly list and download data stored in GCS for use in Python machine learning applications. The core functionalities of this client can be broken down into two key parts.

## Fast List

The fast list component of this client leverages Python multiprocessing to parallelize the listing of files within a GCS bucket. It does this by implementing a workstealing algorithm, where each worker in the list operation is able to steal work from its siblings once it has finished all currently slated listing work. This parallelization leads to a real world speed increase up to 10 times faster than sequential listing. Note that paralellization is limited by the machine on which the client runs, and optimal performance is typically found with a worker count that is 1:1 with the available cores. Benchmarking has demonstrated that the larger the object count, the better Dataflux performs when compared to a linear listing.

### Example Code
```python
from dataflux_core import fast_list

number_of_workers = 20
project = "MyProject"
bucket = "TargetBucket"
target_folder_prefix = "folder1/"

print("Fast list operation starting...")
list_result = fast_list.ListingController(
    max_parallelism=number_of_workers,
    project=project,
    bucket=bucket,
    prefix=target_folder_prefix,
).run()
```

#### Storage Class

By default, fast list will only list objects of STANDARD class in GCS buckets. This can be overridden by passing in a string list of storage classes to include while running the Listing Controller. Note that this default behavior was chosen to avoid the cost associated with downloading non-standard GCS classes. Details on GCS Storage Classes can be further explored in the [Storage Class Documentation](https://cloud.google.com/storage/docs/storage-classes).

### Fast List Benchmark Results
|File Count|VM Core Count|List Time Without Dataflux|List Time With Dataflux|
|------------|-------------|--------------------------|-----------------------|
|17944239 Obj|48 Core      |1630.75s                  |79.55s                 |
|5000000 Obj |48 Core      |289.95s                   |23.43s                 |
|1999002 Obj |48 Core      |117.61s                   |12.45s                 |
|578411 Obj  |48 Core      |30.70s                    |9.39s                  |
|10013 Obj   |48 Core      |2.35s                     |6.06s                  |

## Compose Download

The compose download component of the client uses the results of the fast list to efficiently download the files necessary for a machine learning workload. When downloading files from remote stores, small file size often bottlenecks the speed at which files can be downloaded. To avoid this bottleneck, compose download leverages the ability of GCS buckets to concatinate small files into larger composed files in GCS prior to downloading. This greatly improves download performance, particularly on datasets with very large numbers of small files.

### Example Code
```python
from dataflux_core import download

# The maximum size in bytes of a composite download object.
# If this value is set to 0, no composition will occur.
max_compose_bytes = 10000000
project = "MyProject"
bucket = "TargetBucket"

download_params = download.DataFluxDownloadOptimizationParams(
    max_compose_bytes
)

print("Download operation starting...")
download_result = download.dataflux_download(
    project_name=project,
    bucket_name=bucket,
    # The list_results parameter is the value returned by fast list in the previous code example.
    objects=list_result,
    dataflux_download_optimization_params=download_params,
)
```

#### Multiple Download Options

Looking at the [download code](dataflux_core/download.py) you will notice three distinct download functions. The default function used in the dataflux-pytorch client is `dataflux_download`. The other functions serve to improve performance for specific use cases.

###### Parallel Download

The `dataflux_download_parallel` function is the most performant stand-alone download function. When using the dataflux client library in isolation, this is the recommended download function. Parallelization must be tuned based on available CPU power and network bandwidth.

###### Threaded Download

The `dataflux_download_threaded` function allows for some amount of downlod parallelization while running within daemonic processes (e.g. a distributed ML workload leveraging [ray](https://www.ray.io/)). Daemonic processes are not permitted to spin up child processes, and thus threading must be used in these instances. Threading download performance is similar to that of multiprocessing for most use-cases, but loses out on performance as the thread/process count increases. Additionally, threading does not allow for signal interuption, so SIGINT cleanup triggers are disabled when running a threaded download.

## Getting Started

To get started leveraging the dataflux client library, we encourage you to start from the [Dataflux Dataset for Pytorch](https://github.com/GoogleCloudPlatform/dataflux-pytorch). For an example of client-specific implementation, please see the [benchmark code](dataflux_core/benchmarking/dataflux_client_bench.py).

## Support

* Please file a GitHub issue in this repository
* If you need to get in touch with us, email dataflux-customer-support@google.com

## Contributing

We welcome your feedback, issues, and bug fixes. We have a tight roadmap at this time so if you have a major feature or change in functionality you'd like to contribute, please open a GitHub Issue for discussion prior to sending a pull request. Please see [CONTRIBUTING](docs/contributing.md) for more information on how to report bugs or submit pull requests.

## Code of Conduct

This project has adopted the Google Open Source Code of Conduct. Please see [code-of-conduct.md](docs/code-of-conduct.md) for more information.

## License

The Dataflux Python Client has an Apache License 2.0. Please see the [LICENSE](LICENSE) file for more information.
