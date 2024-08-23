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

from __future__ import annotations

import logging
import multiprocessing
import queue
import time

from dataflux_core import range_splitter, user_agent
from dataflux_core.download import COMPOSED_PREFIX
from google.api_core.client_info import ClientInfo
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

DEFAULT_ALLOWED_CLASS = ["STANDARD"]
MODIFIED_RETRY = DEFAULT_RETRY.with_deadline(300.0).with_delay(initial=1.0,
                                                               multiplier=1.2,
                                                               maximum=45.0)


def remove_prefix(text: str, prefix: str):
    """Helper function that removes prefix from a string.

    Args:
        text: String of text to trim a prefix from.
        prefix: String of text that will be trimmed from text.

    Returns:
        Text value with the specified prefix removed.
    """
    # Note that as of python 3.9 removeprefix is built into string.
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


class ListWorker(object):
    """Worker that lists a range of objects from a GCS bucket.

    Attributes:
        name: String name of the worker.
        gcs_project: The string name of the google cloud storage project to list from.
        bucket: The string name of the storage bucket to list from.from . import fast_list, download
        send_work_stealing_needed_queue: Multiprocessing queue pushed to when a worker needs more work.
        heartbeat_queue: Multiprocessing queue pushed to indicating worker is running nominally.
        direct_work_available_queue: Multiprocessing queue to push availble work stealing ranges to.
        idle_queue: Multiprocessing queue pushed to when worker is waiting for new work to steal.
        unidle_queue: Multiprocessing queue pushed to when the worker has successfully stolen work.
        results_queue: Multiprocessing queue on which the worker pushes its listing results onto.
        metadata_queue: Multiprocessing queue on which the worker pushes tracking metadata.
        start_range: Stirng start range worker will begin listing from.
        end_range: String end range worker will list until.
        retry_config: The retry parameter to supply to list_blob.

        results: Set storing aggregate results prior to pushing onto results_queue.
        client: The GCS client through which all GCS list operations are executed.
        skip_compose: When true, skip listing files with the composed object prefix.
        list_directory_objects: When true, include files with names ending in "/" in the listing. Default false.
        prefix: When provided, only list objects under this prefix.
        allowed_storage_classes: The set of GCS Storage Class types fast list will include.
        max_results: The maximum results per list call (set to max page size of 5000).
        splitter: The range_splitter object used by this worker to divide work.
        default_alph: The baseline alphabet used to initialize the range_splitter.
        api_call_count: Variable tracking the number of GCS list calls made by the worker.
    """

    def __init__(
        self,
        name: str,
        gcs_project: str,
        bucket: str,
        send_work_stealing_needed_queue: "multiprocessing.Queue[str]",
        heartbeat_queue: "multiprocessing.Queue[str]",
        direct_work_available_queue: "multiprocessing.Queue[tuple[str, str]]",
        idle_queue: "multiprocessing.Queue[str]",
        unidle_queue: "multiprocessing.Queue[str]",
        results_queue: "multiprocessing.Queue[set[tuple[str, int]]]",
        metadata_queue: "multiprocessing.Queue[tuple[str, int]]",
        error_queue: "multiprocessing.Queue[Exception]",
        start_range: str,
        end_range: str,
        retry_config:
        "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
        client: storage.Client = None,
        skip_compose: bool = True,
        list_directory_objects: bool = False,
        prefix: str = "",
        allowed_storage_classes: list[str] = DEFAULT_ALLOWED_CLASS,
        max_retries: int = 5,
    ):
        self.name = name
        self.gcs_project = gcs_project
        self.bucket = bucket
        self.send_work_stealing_needed_queue = send_work_stealing_needed_queue
        self.heartbeat_queue = heartbeat_queue
        self.direct_work_available_queue = direct_work_available_queue
        self.idle_queue = idle_queue
        self.unidle_queue = unidle_queue
        self.results_queue = results_queue
        self.metadata_queue = metadata_queue
        self.error_queue = error_queue
        self.start_range = start_range
        self.end_range = end_range
        self.results: set[tuple[str, int]] = set()
        self.client = client
        self.max_results = 5000
        self.splitter = None
        self.default_alph = "ab"
        self.skip_compose = skip_compose
        self.list_directory_objects = list_directory_objects
        self.prefix = prefix if prefix else ""
        self.allowed_storage_classes = allowed_storage_classes
        self.api_call_count = 0
        self.max_retries = max_retries
        self.retry_config = retry_config

    def wait_for_work(self) -> bool:
        """Indefinitely waits for available work and consumes it once available.

        Returns:
          Boolean value indicating that new work has been acquired. The function
          will only return False in response to receiving a shutdown signal (None)
          from the controller.
        """
        self.send_work_stealing_needed_queue.put(self.name)
        self.idle_queue.put(self.name)
        logging.debug(f"Process {self.name} waiting for work...")
        while True:
            try:
                self.heartbeat_queue.put(self.name)
                new_range = self.direct_work_available_queue.get_nowait()
                # None is pushed onto the queue as the shutdown signal once all work is finished.
                if new_range[0] != None:
                    self.unidle_queue.put(self.name)
            except queue.Empty:
                time.sleep(0.1)
                continue
            break
        if new_range[0] is None:
            logging.debug(f"Process {self.name} didn't receive work")
            # Upon receiving shutdown signal log all relevant metadata.
            md = (self.name, self.api_call_count)
            self.metadata_queue.put(md)
            return False
        self.start_range = new_range[0]
        self.end_range = new_range[1]
        logging.debug(f"Process {self.name} got new range [{self.start_range},"
                      f" {self.end_range}]")
        return True

    def run(self) -> None:
        """Runs the worker."""
        logging.debug(f"Process {self.name} starting...")
        if not self.client:
            self.client = storage.Client(
                project=self.gcs_project,
                client_info=ClientInfo(user_agent="dataflux/0.0"),
            )
        else:
            user_agent.add_dataflux_user_agent(self.client)
        self.splitter = range_splitter.new_rangesplitter(self.default_alph)
        # When worker has started, attempt to push to all queues. If the idle or unidle queue
        # push fails, the worker will not initialize and will be ignored by the controller.
        # This allows us to safely handle multiprocessing failures that occur on startup.
        self.idle_queue.put(self.name)
        self.unidle_queue.put(self.name)
        self.heartbeat_queue.put(self.name)
        if self.retry_config:
            # Post a heartbeat when retrying so the process doesn't get killed.
            # The retry class automatically logs the retry as a debug log.
            def on_error(e: Exception):
                self.heartbeat_queue.put(self.name)

            self.retry_config._on_error = on_error
        if self.start_range is None and self.end_range is None:
            if not self.wait_for_work():
                return
        retries_remaining = self.max_retries
        while True:
            has_results = False
            try:
                list_blob_args = {
                    "max_results":
                    self.max_results,
                    "start_offset":
                    self.prefix + self.start_range,
                    "end_offset": ("" if not self.end_range else self.prefix +
                                   self.end_range),
                    "retry":
                    self.retry_config,
                }
                if self.prefix:
                    list_blob_args["prefix"] = self.prefix
                blobs = self.client.bucket(
                    self.bucket).list_blobs(**list_blob_args)
                self.api_call_count += 1
                i = 0
                self.heartbeat_queue.put(self.name)
                for blob in blobs:
                    i += 1
                    if ((not self.skip_compose
                         or not blob.name.startswith(COMPOSED_PREFIX)) and
                        (self.list_directory_objects or blob.name[-1] != "/")
                            and blob.storage_class
                            in self.allowed_storage_classes):
                        self.results.add((blob.name, blob.size))
                    # Remove the prefix from the name so that range calculations remain prefix-agnostic.
                    # This is necessary due to the unbounded end-range when splitting string namespaces
                    # of unknown size.
                    self.start_range = remove_prefix(blob.name, self.prefix)
                    if i == self.max_results:
                        # Only allow work stealing when paging.
                        has_results = True
                        break
                retries_remaining = self.max_retries
            except Exception as e:
                retries_remaining -= 1
                logging.error(
                    f"process {self.name} encountered error ({retries_remaining} retries left): {str(e)}"
                )
                if retries_remaining == 0:
                    logging.error("process " + self.name +
                                  " is out of retries; exiting")
                    self.error_queue.put(e)
                    return
                continue
            if has_results:
                # Check for work stealing.
                try:
                    self.send_work_stealing_needed_queue.get_nowait()
                except queue.Empty:
                    continue
                split_points = self.splitter.split_range(
                    self.start_range, self.end_range, 1)
                steal_range = (split_points[0], self.end_range)
                self.direct_work_available_queue.put(steal_range)
                self.end_range = split_points[0]
                self.max_results = 5000
            else:
                # All done, wait for work.
                if len(self.results) > 0:
                    self.results_queue.put(self.results)
                    self.results = set()
                if not self.wait_for_work():
                    return


def run_list_worker(
    name: str,
    gcs_project: str,
    bucket: str,
    send_work_stealing_needed_queue: "multiprocessing.Queue[str]",
    heartbeat_queue: "multiprocessing.Queue[str]",
    direct_work_available_queue: "multiprocessing.Queue[tuple[str, str]]",
    idle_queue: "multiprocessing.Queue[str]",
    unidle_queue: "multiprocessing.Queue[str]",
    results_queue: "multiprocessing.Queue[set[tuple[str, int]]]",
    metadata_queue: "multiprocessing.Queue[tuple[str, int]]",
    error_queue: "multiprocessing.Queue[Exception]",
    start_range: str,
    end_range: str,
    retry_config: "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
    client: storage.Client = None,
    skip_compose: bool = True,
    prefix: str = "",
    allowed_storage_classes: list[str] = DEFAULT_ALLOWED_CLASS,
) -> None:
    """Helper function to execute a ListWorker.

    Args:
      name: String name of the list worker.
      gcs_project: String name of the google cloud project in use.
      bucket: String name of the google cloud bucket to list from.
      send_work_stealing_needed_queue: Multiprocessing queue pushed to when a worker needs more work.
      heartbeat_queue: Multiprocessing queue pushed to while a worker is running nominally.
      direct_work_available_queue: Multiprocessing queue to push availble work stealing ranges to.
      idle_queue: Multiprocessing queue pushed to when worker is waiting for new work to steal.
      unidle_queue: Multiprocessing queue pushed to when the worker has successfully stolen work.
      results_queue: Multiprocessing queue on which the worker pushes its listing results onto.
      metadata_queue: Multiprocessing queue on which the worker pushes tracking metadata.
      error_queue: Multiprocessing queue to track errors from the worker process.
      start_range: String start range worker will begin listing from.
      end_range: String end range worker will list until.
      retry_config: The retry parameter to supply to list_blob.
      client: The GCS storage client. When not provided, will be derived from background auth.
      skip_compose: When true, skip listing files with the composed object prefix.
      prefix: When provided, only list objects under this prefix.
      allowed_storage_classes: The set of GCS Storage Class types fast list will include.
    """
    ListWorker(
        name,
        gcs_project,
        bucket,
        send_work_stealing_needed_queue,
        heartbeat_queue,
        direct_work_available_queue,
        idle_queue,
        unidle_queue,
        results_queue,
        metadata_queue,
        error_queue,
        start_range,
        end_range,
        retry_config,
        client,
        skip_compose=skip_compose,
        prefix=prefix,
        allowed_storage_classes=allowed_storage_classes,
    ).run()


class ListingController(object):
    """This controller manages and monitors all listing workers operating on the GCS bucket.

    Attributes:
        max_parallelism: The maximum number of processes to start via the Multiprocessing library.
        gcs_project: The string name of the google cloud storage project to list from.
        bucket: The string name of the storage bucket to list from.
        inited: The set of ListWorker processes that have succesfully started.
        checkins: A dictionary tracking the last known checkin time for each inited ListWorker.
        waiting_for_work: The number of ListWorker processes currently waiting for new listing work.
        sort_results: Boolean indicating whether the final result set should be sorted or unsorted.
        skip_compose: When true, skip listing files with the composed object prefix.
        prefix: When provided, only list objects under this prefix.
        allowed_storage_classes: The set of GCS Storage Class types fast list will include.
        retry_config: The retry config passed to list_blobs.
    """

    def __init__(
        self,
        max_parallelism: int,
        project: str,
        bucket: str,
        sort_results: bool = False,
        skip_compose: bool = True,
        prefix: str = "",
        allowed_storage_classes: list[str] = DEFAULT_ALLOWED_CLASS,
        retry_config=MODIFIED_RETRY,
    ):
        # The maximum number of threads utilized in the fast list operation.
        self.max_parallelism = max_parallelism
        self.gcs_project = project
        self.bucket = bucket
        self.inited = set()
        self.checkins = {}
        self.waiting_for_work = 0
        self.sort_results = sort_results
        self.client = None
        self.skip_compose = skip_compose
        self.prefix = prefix
        self.allowed_storage_classes = allowed_storage_classes
        self.retry_config = retry_config

    def manage_tracking_queues(
        self,
        idle_queue: "multiprocessing.Queue[str]",
        unidle_queue: "multiprocessing.Queue[str]",
        heartbeat_queue: "multiprocessing.Queue[str]",
    ) -> None:
        """Manages metadata queues to track execution of the listing operation.

        Args:
          idle_queue: the queue workers push to when in need of new work to steal.
          unidle_queue: the queue workers push to when they steal work.
          heartbeat_queue: the queue workers push to continuously while running nominally.
        """
        while True:
            try:
                idle_queue.get_nowait()
                self.waiting_for_work += 1
            except queue.Empty:
                break
        while True:
            try:
                unidle_queue.get_nowait()
                self.waiting_for_work -= 1
            except queue.Empty:
                break
        while True:
            try:
                inited_worker = heartbeat_queue.get_nowait()
                current_time = time.time()
                self.inited.add(inited_worker)
                self.checkins[inited_worker] = current_time
            except queue.Empty:
                break

    def check_crashed_processes(self) -> bool:
        """Checks if any processes have crashed.

        Returns:
          A boolean indicating if any processes have crashed after initialization.
          If this function returns true, it indicates a need to restart the listing
          operation.
        """
        logging.debug("checking for crashed procs...")
        now = time.time()
        crashed = []
        # Wait at least 60 seconds or 2 times the API call retry delay for check-ins,
        # otherwise processes might appear to be crashed while retrying API calls.
        checkin_wait = 2 * self.retry_config._maximum if self.retry_config else 0
        checkin_wait = max(checkin_wait, 60)
        for inited_worker, last_checkin in self.checkins.items():
            if now - last_checkin > checkin_wait:
                crashed.append(inited_worker)
            for proc in crashed:
                if proc in self.inited:
                    logging.error(
                        "process crash detected, ending list procedure...")
                    return True
        return False

    def cleanup_processes(
        self,
        processes: "list[multiprocessing.Process]",
        results_queue: "multiprocessing.Queue[set[tuple[str, int]]]",
        metadata_queue: "multiprocessing.Queue[tuple[str, int]]",
        results: "set[tuple[str, int]]",
    ) -> list[tuple[str, int]]:
        """Allows processes to shut down, kills procs that failed to initialize.

        Args:
          processes: the list of processes.
          results_queue: the queue for transmitting all result tuples from listing.
          metadata_queue: the queue for transmitting all tracking metadata from workers.
          results: the set of unique results consumed from results_queue.

        Returns:
          A sorted list of (str, int) tuples indicating the name and file size of each
          unique file listed in the listing process.

        """
        api_call_count = 0
        while True:
            alive = False
            live_procs = 0
            for p in processes:
                if p.is_alive():
                    alive = True
                    live_procs += 1
                    while True:
                        try:
                            result = results_queue.get_nowait()
                            results.update(result)
                            logging.debug(f"Result count: {len(results)}")
                        except queue.Empty:
                            break
                    time.sleep(0.2)
                    break
            while True:
                try:
                    metadata = metadata_queue.get_nowait()
                    api_call_count += metadata[1]
                except queue.Empty:
                    break
            logging.debug("Live procs: %d", live_procs)
            logging.debug("Inited procs: %d", len(self.inited))
            if live_procs <= self.max_parallelism - len(self.inited):
                alive = False
                # This prevents any memory leaks from multiple executions, but does kill
                # the stuck processes very aggressively. It does not cause issues in
                # execution, but looks very loud to the user if they are watching debug
                # output.
                for p in processes:
                    if p.is_alive():
                        p.terminate()
            if not alive:
                logging.debug(f"Total GCS API call count: {api_call_count}")
                if self.sort_results:
                    return sorted(results)
                return list(results)

    def terminate_now(
            self, processes: "list[multiprocessing.Process]") -> RuntimeError:
        """Terminates all processes immediately.

        Args:
          processes: The full list of multiprocessing processes.

        Returns:
            RuntimeError indicating that one or more multiprocess processes has
            become unresponsive
        """
        for p in processes:
            p.terminate()
        raise RuntimeError(
            "multiprocessing child process became unresponsive; check logs for underlying error"
        )

    def run(self) -> list[tuple[str, int]]:
        """Runs the controller that manages fast listing.

        Returns:
          A sorted list of (str, int) tuples indicating the name and file size of each
          unique file listed in the listing process.
        """
        # Define the queues.
        send_work_stealing_needed_queue: multiprocessing.Queue[str] = (
            multiprocessing.Queue())
        heartbeat_queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        direct_work_available_queue: multiprocessing.Queue[tuple[str, str]] = (
            multiprocessing.Queue())
        idle_queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        unidle_queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        results_queue: multiprocessing.Queue[set[tuple[str, int]]] = (
            multiprocessing.Queue())
        metadata_queue: multiprocessing.Queue[tuple[
            str, int]] = multiprocessing.Queue()
        error_queue: multiprocessing.Queue[Exception] = multiprocessing.Queue()
        processes = []
        results: set[tuple[str, int]] = set()
        for i in range(self.max_parallelism):
            p = multiprocessing.Process(
                target=run_list_worker,
                args=(
                    "dataflux-listing-proc." + str(i),
                    self.gcs_project,
                    self.bucket,
                    send_work_stealing_needed_queue,
                    heartbeat_queue,
                    direct_work_available_queue,
                    idle_queue,
                    unidle_queue,
                    results_queue,
                    metadata_queue,
                    error_queue,
                    "" if i == 0 else None,
                    "" if i == 0 else None,
                    self.retry_config,
                    self.client,
                    self.skip_compose,
                    self.prefix,
                    self.allowed_storage_classes,
                ),
            )
            processes.append(p)
            p.start()
            # Wait before starting the next process to avoid deadlock when multiple processes
            # attempt to register with the same multiprocessing queue.
            time.sleep(0.1)
        while True:
            time.sleep(0.2)
            try:
                e = error_queue.get_nowait()
                logging.error(
                    f"Got error from child process; exiting. Check child process logs for more details. Error: {e}"
                )
                return self.terminate_now(processes)
            except queue.Empty:
                pass
            alive = False
            for p in processes:
                if p.is_alive():
                    alive = True
                    break
            new_results = set()
            while True:
                try:
                    result = results_queue.get_nowait()
                    new_results.update(result)
                except queue.Empty:
                    break
            if len(new_results) > 0:
                results.update(new_results)
                logging.debug(f"Result count: {len(results)}")
            if not alive:
                break
            # Update all queues related to tracking process status.
            self.manage_tracking_queues(idle_queue, unidle_queue,
                                        heartbeat_queue)
            if self.check_crashed_processes():
                return self.terminate_now(processes)
            logging.debug("Inited procs: %d", len(self.inited))
            logging.debug("Waiting for work: %d", self.waiting_for_work)
            if len(self.inited) == self.waiting_for_work and (
                    self.waiting_for_work > 0):
                logging.debug("Exiting, all processes are waiting for work")
                for _ in range(self.max_parallelism * 2):
                    direct_work_available_queue.put((None, None))
                break
        while True:
            try:
                result = results_queue.get_nowait()
                results.update(result)
                logging.debug(f"Result count: {len(results)}")
            except queue.Empty:
                break
        logging.debug("Got all results, waiting for processes to exit.")
        return self.cleanup_processes(processes, results_queue, metadata_queue,
                                      results)
