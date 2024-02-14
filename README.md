This is the git repository for the Dataflux Python client library.

# Overview

This is the client library backing the [Dataflux Dataset for Pytorch](https://github.com/GoogleCloudPlatform/dataflux-pytorch). The purpose of this client is to quickly list and download data stored in GCS for use in Python machine learning applications. The core functionalities of this client can be broken down into two key parts.

## Fast List

The fast list component of this client leverages Python multiprocessing to parallelize the listing of files within a GCS bucket. It does this by implementing a workstealing algorithm, where each worker in the list operation is able to steal work from its siblings once it has finished all currently slated listing work. This parallelization leads to a real world speed increase up to 10 times faster than sequential listing. Note that paralellization is limited by the machine on which the client runs, and optimal performance is typically found with a worker count that is 1:1 with the available cores.

## Compose Download

The compose download component of the client uses the results of the fast list to efficiently download the files necessary for a machine learning workload. When downloading files from remote stores, small file size often bottlenecks the speed at which files can be downloaded. To avoid this bottleneck, compose download leverages the ability of GCS buckets to concatinate small files into larger composed files in GCS prior to downloading. This greatly improves download performance, particularly on datasets with very large numbers of small files.

## Getting Started

To get started leveraging the dataflux client library, we encourage you to start from the [Dataflux Dataset for Pytorch](https://github.com/GoogleCloudPlatform/dataflux-pytorch). For an example of client-specific implementation, please see the [benchmark code](dataflux_core/benchmarking/dataflux_client_bench.py).

# Support

* dataflux-customer-support@google.com

# Contributing

We welcome your feedback, issues, and bug fixes. We have a tight roadmap at this time so if you have a major feature or change in functionality you'd like to contribute, please open a GitHub Issue for discussion prior to sending a pull request. Please see [CONTRIBUTING](docs/contributing.md) for more information on how to report bugs or submit pull requests.

# Code of Conduct

This project has adopted the Google Open Source Code of Conduct. Please see [code-of-conduct.md](docs/code-of-conduct.md) for more information.

# License

The Dataflux Python Client has an Apache License 2.0. Please see the [LICENSE](LICENSE) file for more information.
