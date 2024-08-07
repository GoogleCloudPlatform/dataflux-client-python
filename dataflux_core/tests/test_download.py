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
from unittest import mock

from dataflux_core import download
from dataflux_core.tests import fake_gcs


class DownloadTestCase(unittest.TestCase):

    def test_compose(self):
        bucket_name = "test_bucket"
        destination_blob_name = "dest_name"
        objects = [("one", 3), ("two", 3), ("three", 5)]
        client = fake_gcs.Client()
        bucket = client.bucket(bucket_name)
        bucket._add_file("one", bytes("one", "utf-8"))
        bucket._add_file("two", bytes("two", "utf-8"))
        bucket._add_file("three", bytes("three", "utf-8"))
        expected_result = b"onetwothree"
        blob = download.compose("", bucket_name, destination_blob_name,
                                objects, client)
        self.assertEqual(blob.name, destination_blob_name)
        self.assertEqual(blob.content, expected_result)
        self.assertIn("dataflux", client._connection.user_agent)

    def test_decompose(self):
        bucket_name = "test_bucket"
        object_name = "test_obj"
        objects = [("one", 3), ("two", 3), ("three", 5)]
        client = fake_gcs.Client()
        bucket = client.bucket(bucket_name)
        bucket._add_file(object_name, bytes("onetwothree", "utf-8"))
        result = download.decompose("", bucket_name, object_name, objects,
                                    client)
        self.assertEqual(result, [b"one", b"two", b"three"])
        self.assertIn("dataflux", client._connection.user_agent)

    def test_download_single(self):
        client = fake_gcs.Client()
        bucket_name = "test_bucket"
        object_name = "test_obj"
        content = bytes("onetwothree", "utf-8")
        bucket = client.bucket(bucket_name)
        bucket._add_file(object_name, content)
        result = download.download_single(client, bucket_name, object_name)
        self.assertEqual(result, content)

    def test_dataflux_download(self):
        bucket_name = "test_bucket"
        objects = [("one", 3), ("two", 3), ("three", 5)]
        client = fake_gcs.Client()
        bucket = client.bucket(bucket_name)
        bucket._add_file("one", bytes("one", "utf-8"))
        bucket._add_file("two", bytes("two", "utf-8"))
        bucket._add_file("three", bytes("three", "utf-8"))
        params = download.DataFluxDownloadOptimizationParams(32)
        expected_result = [b"one", b"two", b"three"]
        result = download.dataflux_download("", bucket_name, objects, client,
                                            params)
        self.assertEqual(result, expected_result)
        # This checks for succesful deletion of the composed object.
        if len(bucket.blobs) != 3:
            self.fail(
                f"expected only 3 objects in bucket, but found {len(bucket.blobs)}"
            )
        self.assertIn("dataflux", client._connection.user_agent)

    def test_dataflux_download_parallel(self):
        test_cases = [
            {
                "name": "exceed number of items",
                "procs": 4
            },
            {
                "name": "single proc",
                "procs": 1
            },
            {
                "name": "standard",
                "procs": 2
            },
        ]
        bucket_name = "test_bucket"
        objects = [("one", 3), ("two", 3), ("three", 5)]
        client = fake_gcs.Client()
        bucket = client.bucket(bucket_name)
        bucket._add_file("one", bytes("one", "utf-8"))
        bucket._add_file("two", bytes("two", "utf-8"))
        bucket._add_file("three", bytes("three", "utf-8"))
        params = download.DataFluxDownloadOptimizationParams(32)
        expected_result = [b"one", b"two", b"three"]
        for tc in test_cases:
            result = download.dataflux_download_parallel(
                "",
                bucket_name,
                objects,
                client,
                params,
                tc["procs"],
            )
            self.assertEqual(result, expected_result)
            # This checks for succesful deletion of the composed object.
            if len(bucket.blobs) != 3:
                self.fail(
                    f"{tc['name']} expected only 3 objects in bucket, but found {len(bucket.blobs)}"
                )

    def test_dataflux_download_threaded(self):
        test_cases = [
            {
                "name": "exceed number of items",
                "threads": 4
            },
            {
                "name": "single thread",
                "threads": 1
            },
            {
                "name": "standard",
                "threads": 2
            },
        ]
        bucket_name = "test_bucket"
        objects = [("one", 3), ("two", 3), ("three", 5)]
        client = fake_gcs.Client()
        bucket = client.bucket(bucket_name)
        bucket._add_file("one", bytes("one", "utf-8"))
        bucket._add_file("two", bytes("two", "utf-8"))
        bucket._add_file("three", bytes("three", "utf-8"))
        params = download.DataFluxDownloadOptimizationParams(32)
        expected_result = [b"one", b"two", b"three"]
        for tc in test_cases:
            result = download.dataflux_download_threaded(
                "",
                bucket_name,
                objects,
                client,
                params,
                tc["threads"],
            )
            self.assertEqual(result, expected_result)
            # This checks for succesful deletion of the composed object.
            if len(bucket.blobs) != 3:
                self.fail(
                    f"{tc['name']} expected only 3 objects in bucket, but found {len(bucket.blobs)}"
                )
        self.assertIn("dataflux", client._connection.user_agent)

    def test_dataflux_download_lazy(self):
        test_cases = [
            {
                "desc": "Need to compose objects before downloading",
                "max_composite_object_size": 100,
            },
            {
                "desc": "Do not need to compose objects before downloading",
                "max_composite_object_size": 0,
            },
        ]

        for tc in test_cases:
            bucket_name = "test_bucket"
            objects = [("one", 3), ("two", 3), ("three", 5)]
            client = fake_gcs.Client()
            bucket = client.bucket(bucket_name)
            bucket._add_file("one", bytes("one", "utf-8"))
            bucket._add_file("two", bytes("two", "utf-8"))
            bucket._add_file("three", bytes("three", "utf-8"))
            params = download.DataFluxDownloadOptimizationParams(
                tc["max_composite_object_size"])
            expected_result = [b"one", b"two", b"three"]
            result = download.dataflux_download_lazy("", bucket_name, objects,
                                                     client, params)
            self.assertEqual(
                list(result),
                expected_result,
                f"test {tc['desc']} got {list(result)} objects, wanted {expected_result}",
            )
            # This checks for succesful deletion of the composed object.
            if len(bucket.blobs) != 3:
                self.fail(
                    f"test {tc['desc']} expected only 3 objects in bucket, but found {len(bucket.blobs)}"
                )
            self.assertIn("dataflux", client._connection.user_agent)

    def test_clean_composed_object(self):

        class ComposedObj:

            def __init__(self):
                self.deleted = False

            def delete(self, retry=None):
                self.deleted = True

        current_composed_object = ComposedObj()
        download.clean_composed_object(current_composed_object)
        if not current_composed_object.deleted:
            self.fail("expected composed object cleanup: True, got False")


if __name__ == "__main__":
    unittest.main()
