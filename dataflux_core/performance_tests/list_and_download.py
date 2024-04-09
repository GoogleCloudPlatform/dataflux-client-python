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
from math import ceil

FIFTY_GB = 50000000000


class ClientPerformanceTest(unittest.TestCase):
    def get_config(self):
        config = {}
        # Gather env vars into dictionary.
        config["project"] = os.getenv("PROJECT")
        config["bucket"] = os.getenv("BUCKET")
        config["prefix"] = os.getenv("PREFIX")
        config["num_workers"] = os.getenv("LIST_WORKERS")
        config["expected_file_count"] = os.getenv("FILE_COUNT")
        config["expected_total_size"] = os.getenv("TOTAL_FILE_SIZE")
        config["max_compose_bytes"] = os.getenv("MAX_COMPOSE_BYTES")
        config["list_timeout"] = os.getenv("LIST_TIMEOUT")
        config["download_timeout"] = os.getenv("DOWNLOAD_TIMEOUT")
        config["parallelization"] = os.getenv("PARALLELIZATION")

        # Type convert env vars.
        if config["num_workers"]:
            config["num_workers"] = int(config["num_workers"])
        if config["expected_file_count"]:
            config["expected_file_count"] = int(config["expected_file_count"])
        if config["expected_total_size"]:
            config["expected_total_size"] = int(config["expected_total_size"])
        config["max_compose_bytes"] = (
            int(config["max_compose_bytes"])
            if config["max_compose_bytes"]
            else 100000000
        )
        if config["list_timeout"]:
            config["list_timeout"] = float(config["list_timeout"])
        if config["download_timeout"]:
            config["download_timeout"] = float(config["download_timeout"])
        config["parallelization"] = (
            int(config["parallelization"]) if config["parallelization"] else 1
        )

        return config

    def run_list(self, config):
        list_start_time = time.time()
        list_result = fast_list.ListingController(
            config["num_workers"],
            config["project"],
            config["bucket"],
            prefix=config["prefix"],
        ).run()
        list_end_time = time.time()
        listing_time = list_end_time - list_start_time
        if (
            config["expected_file_count"]
            and len(list_result) != config["expected_file_count"]
        ):
            raise AssertionError(
                f"Expected {config['expected_file_count']} files, but got {len(list_result)}"
            )
        if config["list_timeout"] and listing_time > config["list_timeout"]:
            raise AssertionError(
                f"Expected list operation to complete in under {config['list_timeout']} seconds, but took {listing_time} seconds."
            )
        return list_result

    def run_download(self, config, list_result, segmented=False):
        download_params = download.DataFluxDownloadOptimizationParams(
            config["max_compose_bytes"]
        )
        download_start_time = time.time()
        download_result = None
        if config["parallelization"] and config["parallelization"] > 1:
            download_result = download.dataflux_download_parallel(
                config["project"],
                config["bucket"],
                list_result,
                dataflux_download_optimization_params=download_params,
                parallelization=config["parallelization"],
            )
        else:
            download_result = download.dataflux_download(
                config["project"],
                config["bucket"],
                list_result,
                dataflux_download_optimization_params=download_params,
            )
        download_end_time = time.time()
        downloading_time = download_end_time - download_start_time
        total_size = sum([len(x) for x in download_result])
        if (
            not segmented
            and config["expected_total_size"]
            and total_size != config["expected_total_size"]
        ):
            raise AssertionError(
                f"Expected {config['expected_total_size']} bytes but got {total_size} bytes"
            )
        if config["download_timeout"] and downloading_time > config["download_timeout"]:
            raise AssertionError(
                f"Expected download operation to complete in under {config['download_timeout']} seconds, but took {downloading_time} seconds."
            )
        return total_size

    def test_list_and_download_one_shot(self):
        config = self.get_config()
        list_result = self.run_list(config)
        self.run_download(config, list_result)

    def test_list_and_download_segmented(self):
        # This function is needed to avoid OOM errors when the dataset size
        # exceeds the memory of the VM.
        config = self.get_config()
        list_result = self.run_list(config)
        num_segments = config["expected_total_size"] / FIFTY_GB
        segment_size = ceil(config["expected_file_count"] / num_segments)
        segments = [
            list_result[i : i + segment_size]
            for i in range(0, len(list_result), segment_size)
        ]
        total_size = 0
        for seg in segments:
            total_size += self.run_download(config, seg, segmented=True)
        if (
            config["expected_total_size"]
            and total_size != config["expected_total_size"]
        ):
            raise AssertionError(
                f"Expected {config['expected_total_size']} bytes but got {total_size} bytes"
            )
