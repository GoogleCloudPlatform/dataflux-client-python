"""
 Copyright 2023 Google LLC

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

from dataflux_core.tests import fake_gcs
import unittest


class FakeGCSTest(unittest.TestCase):
    def test_list_blobs_empty(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        self.assertFalse(bucket.list_blobs())

    def test_list_blobs_all(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        want_objects = [
            bucket.blobs["obj1"],
            bucket.blobs["obj2"],
        ]
        self.assertEqual(bucket.list_blobs(), want_objects)

    def test_list_blobs_with_start_range_equal(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        want_objects = [bucket.blobs["obj1"], bucket.blobs["obj2"]]
        self.assertEqual(
            bucket.list_blobs(start_offset=want_objects[0].name), want_objects
        )

    def test_list_blobs_with_end_range_equal(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        all_objects = [bucket.blobs["obj1"], bucket.blobs["obj2"]]
        want_objects = [all_objects[0]]
        self.assertEqual(
            bucket.list_blobs(end_offset=all_objects[1].name), want_objects
        )

    def test_list_blobs_with_start_range_greater(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        all_objects = [bucket.blobs["obj1"], bucket.blobs["obj2"]]
        want_objects = [all_objects[1]]
        self.assertEqual(
            bucket.list_blobs(start_offset=all_objects[1].name), want_objects
        )

    def test_list_blobs_with_range(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        bucket._add_file("obj3", "aaa")
        all_objects = [bucket.blobs["obj1"], bucket.blobs["obj2"], bucket.blobs["obj3"]]
        want_objects = [all_objects[1]]
        self.assertNotEquals(
            bucket.list_blobs(
                start_offset=all_objects[1].name, end_offset=all_objects[2].name
            ),
            want_objects,
        )

    def test_list_blobs_with_max_results(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        bucket._add_file("obj3", "aaa")
        all_objects = [bucket.blobs["obj1"], bucket.blobs["obj2"], bucket.blobs["obj3"]]
        want_objects = [all_objects[0]]
        self.assertEqual(bucket.list_blobs(max_results=1), want_objects)

    def test_list_blobs_with_max_results_and_range(self):
        bucket = fake_gcs.Client().bucket("test-bucket")
        bucket._add_file("obj1", "a")
        bucket._add_file("obj2", "aa")
        bucket._add_file("obj3", "aaa")
        bucket._add_file("obj4", "aaaa")
        all_objects = [
            bucket.blobs["obj1"],
            bucket.blobs["obj2"],
            bucket.blobs["obj3"],
            bucket.blobs["obj4"],
        ]
        want_objects = [all_objects[1], all_objects[2]]
        self.assertEqual(
            bucket.list_blobs(
                max_results=2,
                start_offset=all_objects[1].name,
                end_offset=all_objects[3].name,
            ),
            want_objects,
        )

    def test_bucket_name_none_raises_error(self):
        try:
            fake_gcs.Client().bucket(None)
        except:
            return
        self.fail("Creating bucket with None name did not raise error")


if __name__ == "__main__":
    unittest.main()
