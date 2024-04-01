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

import queue
import unittest
from dataflux_core import fast_list
from dataflux_core.tests import fake_gcs, fake_multiprocess
import time


class FastListTest(unittest.TestCase):
    def test_single_worker(self):
        """End to end test of a single ListWorker."""
        test_cases = [
            {
                "desc": "List 10k objects default",
                "object_count": 10000,
                "compose_obj_count": 1,
                "prefix_obj_count": 0,
                "archive_obj_count": 0,
                "prefix": "",
                "object_size": 10,
                "directory_obj_count": 10,
                "skip_compose": True,
                "list_directory_objects": False,
                "expected_objects": 10000,
                "expected_api_calls": 3,
            },
            {
                "desc": "List 10k objects including compose",
                "object_count": 10000,
                "compose_obj_count": 1,
                "prefix_obj_count": 0,
                "archive_obj_count": 0,
                "prefix": "",
                "object_size": 10,
                "directory_obj_count": 0,
                "skip_compose": False,
                "list_directory_objects": False,
                "expected_objects": 10001,
                "expected_api_calls": 3,
            },
            {
                "desc": "List 5k objects excluding compose",
                "object_count": 5000,
                "compose_obj_count": 5000,
                "prefix_obj_count": 0,
                "archive_obj_count": 0,
                "prefix": "",
                "object_size": 10,
                "directory_obj_count": 0,
                "skip_compose": True,
                "list_directory_objects": False,
                "expected_objects": 5000,
                "expected_api_calls": 3,
            },
            {
                "desc": "List 2k objects, prefix only",
                "object_count": 5000,
                "compose_obj_count": 5000,
                "prefix_obj_count": 2000,
                "archive_obj_count": 0,
                "prefix": "test-prefix/",
                "object_size": 10,
                "directory_obj_count": 0,
                "skip_compose": True,
                "list_directory_objects": False,
                "expected_objects": 2000,
                "expected_api_calls": 1,
            },
            {
                "desc": "List directory objects",
                "object_count": 10000,
                "compose_obj_count": 0,
                "prefix_obj_count": 0,
                "archive_obj_count": 0,
                "prefix": "",
                "object_size": 10,
                "directory_obj_count": 10,
                "skip_compose": True,
                "list_directory_objects": True,
                "expected_objects": 10010,
                "expected_api_calls": 3,
            },
            {
                "desc": "Skip non-standard class",
                "object_count": 10000,
                "compose_obj_count": 0,
                "prefix_obj_count": 0,
                "archive_obj_count": 1000,
                "prefix": "",
                "object_size": 10,
                "directory_obj_count": 0,
                "skip_compose": True,
                "list_directory_objects": True,
                "expected_objects": 10000,
                "expected_api_calls": 3,
            },
        ]
        for tc in test_cases:
            client = fake_gcs.Client()
            bucket_name = "test_bucket"
            bucket = client.bucket(bucket_name)
            object_count = tc["object_count"]
            object_size = tc["object_size"]
            results_queue = queue.Queue()
            metadata_queue = queue.Queue()
            work_queue = queue.Queue()
            work_queue.put((None, ""))

            for i in range(object_count):
                bucket._add_file(str(i), b"a" * object_size)
            # Add one composed object to make sure it is skipped.
            for i in range(tc["compose_obj_count"]):
                bucket._add_file(
                    f"dataflux-composed-objects/composed{i}.tar", b"a" * object_size
                )
            for i in range(tc["prefix_obj_count"]):
                bucket._add_file(f"{tc['prefix']}file{i}.txt", b"a" * object_size)
            for i in range(tc["directory_obj_count"]):
                bucket._add_file(f"{tc['prefix']}/dir{i}/", b"")
            for i in range(tc["archive_obj_count"]):
                bucket._add_file(
                    f"archive_{i}", b"a" * object_size, storage_class="ARCHIVE"
                )
            list_worker = fast_list.ListWorker(
                "test_worker",
                "",
                bucket_name,
                queue.Queue(),
                queue.Queue(),
                work_queue,
                queue.Queue(),
                queue.Queue(),
                results_queue,
                metadata_queue,
                "",
                "",
                skip_compose=tc["skip_compose"],
                list_directory_objects=tc["list_directory_objects"],
                prefix=tc["prefix"],
            )
            list_worker.client = client
            list_worker.run()
            got_results = set()
            while True:
                try:
                    new_results = results_queue.get_nowait()
                    got_results.update(new_results)
                except queue.Empty:
                    break
            expected_objects = tc["expected_objects"]
            if len(got_results) != expected_objects:
                self.fail(f"got {len(got_results)} results, want {expected_objects}")
            got_total_size = 0
            for result in got_results:
                got_total_size += result[1]
            want_total_size = (
                expected_objects
                - (tc["directory_obj_count"] if tc["list_directory_objects"] else 0)
            ) * object_size
            if got_total_size != want_total_size:
                self.fail(f"got {got_total_size} total size, want {want_total_size}")
            if list_worker.api_call_count != tc["expected_api_calls"]:
                self.fail(f"{list_worker.api_call_count} on test {tc['desc']}")

    def test_manage_tracking_queues(self):
        """Tests that all tracking queues are pushed to properly."""
        controller = fast_list.ListingController(10, "", "")
        idle_queue = queue.Queue()
        idle_queue.put("one")
        idle_queue.put("two")
        idle_queue.put("three")
        unidle_queue = queue.Queue()
        unidle_queue.put("one")
        hb_queue = queue.Queue()
        hb_queue.put("four")
        controller.manage_tracking_queues(idle_queue, unidle_queue, hb_queue)
        if controller.waiting_for_work != 2:
            self.fail(f"got {controller.waiting_for_work} works waiting, want 2")
        if "four" not in controller.inited:
            self.fail(
                "expected inited worker to be tracked, but was not added to inited"
            )
        if "four" not in controller.checkins:
            self.fail(
                "expected hb_queue entry to be tracked in checkins, but was not found"
            )

    def test_check_crashed_processes(self):
        """Tests that crashed processes are correctly discovered and mitigated."""
        controller = fast_list.ListingController(10, "", "")
        controller.inited.add("one")
        controller.checkins["one"] = time.time()
        if controller.check_crashed_processes():
            self.fail(f"expected no crahsed processes, but found crashed process")
        controller.checkins["one"] = time.time() - 100
        if not controller.check_crashed_processes():
            self.fail(
                f"expected crashed process to be detected, but found no crashed processes"
            )

    def test_cleanup_processes(self):
        """Tests that all processes are cleaned up at the end of execution."""
        controller = fast_list.ListingController(10, "", "", True)
        procs = []
        results_queue = queue.Queue()
        metadata_queue = queue.Queue()
        set1 = set()
        set2 = set()
        set1.add(("item", 1))
        set2.add(("item2", 2))
        results_queue.put(set1)
        results_queue.put(set2)
        results_set = set()
        for i in range(5):
            procs.append(fake_multiprocess.FakeProcess(f"proc{i}", False))
        results = controller.cleanup_processes(
            procs, results_queue, metadata_queue, results_set
        )
        if results:
            self.fail("received results when no processes were alive")
        procs = []
        expected = [("item", 1), ("item2", 2)]
        for i in range(5):
            procs.append(fake_multiprocess.FakeProcess(f"proc{i}", True))
        results = controller.cleanup_processes(
            procs, results_queue, metadata_queue, results_set
        )
        self.assertEqual(results, expected)

    def test_terminate_now(self):
        controller = fast_list.ListingController(10, "", "", True)
        procs = []
        term_tracker = []
        proc_count = 5
        for i in range(proc_count):
            procs.append(fake_multiprocess.FakeProcess(f"proc{i}", False, term_tracker))

        with self.assertRaises(RuntimeError):
            controller.terminate_now(procs)

        self.assertEqual(proc_count, len(term_tracker))

    def test_list_controller_e2e(self):
        """Full end to end test of the fast list operation with one worker."""
        client = fake_gcs.Client()
        bucket_name = "test_bucket"
        bucket = client.bucket(bucket_name)
        object_count = 1000
        object_size = 10
        for i in range(object_count):
            bucket._add_file(str(i), "aaaaaaaaaa")
        controller = fast_list.ListingController(1, "", bucket_name, True)
        controller.client = client
        results = controller.run()
        if len(results) != object_count:
            self.fail(f"got {len(results)} results, want {object_count}")
        got_total_size = 0
        for result in results:
            got_total_size += result[1]
        if got_total_size != object_count * object_size:
            self.fail(
                f"got {got_total_size} results, want {object_count * object_size}"
            )

    def test_wait_for_work_success(self):
        """Tests waiting for work when there is still work remaining."""
        client = fake_gcs.Client()
        worker_name = "test_worker"
        bucket_name = "test_bucket"
        send_work_needed_queue = queue.Queue()
        hb_queue = queue.Queue()
        direct_work_queue = queue.Queue()
        idle_queue = queue.Queue()
        unidle_queue = queue.Queue()
        results_queue = queue.Queue()
        metadata_queue = queue.Queue()
        direct_work_queue.put(("y", "z"))

        list_worker = fast_list.ListWorker(
            worker_name,
            "",
            bucket_name,
            send_work_needed_queue,
            hb_queue,
            direct_work_queue,
            idle_queue,
            unidle_queue,
            results_queue,
            metadata_queue,
            "",
            "",
        )
        list_worker.client = client
        result = list_worker.wait_for_work()
        if not result:
            self.fail(f"got {result}, but expected True")
        self.assertEqual(send_work_needed_queue.get_nowait(), worker_name)
        self.assertEqual(idle_queue.get_nowait(), worker_name)
        self.assertEqual(hb_queue.get_nowait(), worker_name)
        self.assertEqual(unidle_queue.get_nowait(), worker_name)
        self.assertEqual(list_worker.start_range, "y")
        self.assertEqual(list_worker.end_range, "z")

    def test_wait_for_work_shutdown(self):
        """Tests that waiting for work correctly detects shutdown signal."""
        client = fake_gcs.Client()
        worker_name = "test_worker"
        bucket_name = "test_bucket"
        send_work_needed_queue = queue.Queue()
        hb_queue = queue.Queue()
        direct_work_queue = queue.Queue()
        idle_queue = queue.Queue()
        unidle_queue = queue.Queue()
        results_queue = queue.Queue()
        metadata_queue = queue.Queue()
        direct_work_queue.put((None, None))

        list_worker = fast_list.ListWorker(
            worker_name,
            "",
            bucket_name,
            send_work_needed_queue,
            hb_queue,
            direct_work_queue,
            idle_queue,
            unidle_queue,
            results_queue,
            metadata_queue,
            "",
            "",
        )
        list_worker.client = client
        result = list_worker.wait_for_work()
        if result:
            self.fail(f"got {result}, but expected False")
        self.assertEqual(send_work_needed_queue.get_nowait(), worker_name)
        self.assertEqual(idle_queue.get_nowait(), worker_name)
        self.assertEqual(hb_queue.get_nowait(), worker_name)
        self.assertRaises(queue.Empty, unidle_queue.get_nowait)

    def test_fast_list_exits_on_error(self):
        """Test of a single ListWorker with an error."""
        client = fake_gcs.Client()
        bucket_name = None
        results_queue = queue.Queue()
        metadata_queue = queue.Queue()
        work_queue = queue.Queue()
        work_queue.put((None, ""))

        list_worker = fast_list.ListWorker(
            "test_worker",
            "",
            bucket_name,
            queue.Queue(),
            queue.Queue(),
            work_queue,
            queue.Queue(),
            queue.Queue(),
            results_queue,
            metadata_queue,
            "",
            "",
        )
        list_worker.client = client
        list_worker.run()
        got_results = set()
        while True:
            try:
                new_results = results_queue.get_nowait()
                got_results.update(new_results)
            except queue.Empty:
                break
        if len(got_results) != 0:
            self.fail(f"got {len(got_results)} results, want 0")


if __name__ == "__main__":
    unittest.main()
