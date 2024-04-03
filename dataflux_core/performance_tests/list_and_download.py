"""
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

import unittest
import time
import os
from dataflux_core import fast_list, download


class ClientPerformanceTest(unittest.TestCase):
    def test_list_and_download(self):
        # Gather Environment Variables.
        project = os.getenv("PROJECT")
        bucket = os.getenv("BUCKET")
        prefix = os.getenv("PREFIX")
        num_workers = os.getenv("LIST_WORKERS")
        expected_file_count = os.getenv("FILE_COUNT")
        expected_file_size = os.getenv("TOTAL_FILE_SIZE")
        max_compose_bytes = os.getenv("MAX_COMPOSE_BYTES")
        list_timeout = os.getenv("LIST_TIMEOUT")
        download_timeout = os.getenv("DOWNLOAD_TIMEOUT")

        # Type convert env vars.
        if num_workers:
            num_workers = int(num_workers)
        if expected_file_count:
            expected_file_count = int(expected_file_count)
        if expected_file_size:
            expected_file_size = int(expected_file_size)
        max_compose_bytes = int(max_compose_bytes) if max_compose_bytes else 100000000
        if list_timeout:
            list_timeout = float(list_timeout)
        if download_timeout:
            download_timeout = float(download_timeout)
        list_start_time = time.time()
        list_result = fast_list.ListingController(
            num_workers, project, bucket, prefix=prefix
        ).run()
        list_end_time = time.time()
        listing_time = list_end_time - list_start_time
        if expected_file_count and len(list_result) != expected_file_count:
            raise AssertionError(
                f"Expected {expected_file_count} files, but got {len(list_result)}"
            )
        if list_timeout and listing_time > list_timeout:
            raise AssertionError(
                f"Expected list operation to complete in under {list_timeout} seconds, but took {listing_time} seconds."
            )
        download_params = download.DataFluxDownloadOptimizationParams(max_compose_bytes)
        download_start_time = time.time()
        download_result = download.dataflux_download(
            project,
            bucket,
            list_result,
            dataflux_download_optimization_params=download_params,
        )
        download_end_time = time.time()
        downloading_time = download_end_time - download_start_time
        total_size = sum([len(x) for x in download_result])
        if expected_file_size and total_size != expected_file_size:
            raise AssertionError(
                f"Expected {expected_file_size} bytes but got {total_size} bytes"
            )
        if download_timeout and downloading_time > download_timeout:
            raise AssertionError(
                f"Expected download operation to complete in under {download_timeout} seconds, but took {downloading_time} seconds."
            )
