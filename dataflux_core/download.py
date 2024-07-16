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

from __future__ import annotations

import itertools
import logging
import math
import multiprocessing
import queue
import signal
import sys
import threading
import uuid
from typing import Iterator

from google.api_core.client_info import ClientInfo
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from dataflux_core import user_agent

# https://cloud.google.com/storage/docs/retry-strategy#python.
MODIFIED_RETRY = DEFAULT_RETRY.with_deadline(300.0).with_delay(initial=1.0,
                                                               multiplier=1.2,
                                                               maximum=45.0)

# https://cloud.google.com/storage/docs/composite-objects.
MAX_NUM_OBJECTS_TO_COMPOSE = 32

COMPOSED_PREFIX = "dataflux-composed-objects/"

current_composed_object = None


def compose(
    project_name: str,
    bucket_name: str,
    destination_blob_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    retry_config: "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
) -> object:
    """Compose the objects into a composite object, upload the composite object to the GCS bucket and returns it.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        destination_blob_name: the name of the composite object to be created.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        retry_config: The retry parameter to supply to the compose objects call.

    Returns:
        the "blob" of the composed object.
    """
    if len(objects) > MAX_NUM_OBJECTS_TO_COMPOSE:
        raise ValueError(
            f"{MAX_NUM_OBJECTS_TO_COMPOSE} objects allowed to compose, received {len(objects)} objects."
        )

    if storage_client is None:
        storage_client = storage.Client(project=project_name)
    user_agent.add_dataflux_user_agent(storage_client)

    bucket = storage_client.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)

    sources = list()
    for each_object in objects:
        blob_name = each_object[0]
        sources.append(bucket.blob(blob_name))

    destination.compose(sources, retry=retry_config)

    return destination


def decompose(
    project_name: str,
    bucket_name: str,
    composite_object_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    retry_config: "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
) -> list[bytes]:
    """Decompose the composite objects and return the decomposed objects contents in bytes.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        composite_object_name: the name of the composite object to be created.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        retry_config: The retry parameter supplied to the download_as_bytes call.

    Returns:
        the contents (in bytes) of the decomposed objects.
    """
    if storage_client is None:
        storage_client = storage.Client(project=project_name)
    user_agent.add_dataflux_user_agent(storage_client)

    res = []
    composed_object_content = download_single(
        storage_client,
        bucket_name,
        composite_object_name,
        retry_config=retry_config,
    )

    start = 0
    for each_object in objects:
        blob_size = each_object[1]
        content = composed_object_content[start:start + blob_size]
        res.append(content)
        start += blob_size

    if start != len(composed_object_content):
        logging.error(
            "decomposed object length = %s bytes, wanted = %s bytes.",
            start,
            len(composed_object_content),
        )
    return res


def download_single(
    storage_client: object,
    bucket_name: str,
    object_name: str,
    retry_config: "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
) -> bytes:
    """Download the contents of this object as a bytes object and return it.

    Args:
        storage_client: the google.cloud.storage.Client initialized with the project.
        bucket_name: the name of the GCS bucket that holds the object.
        object_name: the name of the object to download.
        retry_config: The retry parameter supplied to the download_as_bytes call.

    Returns:
        the contents of the object in bytes.
    """
    bucket_handle = storage_client.bucket(bucket_name)
    blob = bucket_handle.blob(object_name)
    return blob.download_as_bytes(retry=retry_config)


class DataFluxDownloadOptimizationParams:
    """Parameters used to optimize DataFlux download performance.

    Attributes:
        max_composite_object_size: An integer indicating a cap for the maximum size of the composite object.

    """

    def __init__(self, max_composite_object_size):
        self.max_composite_object_size = max_composite_object_size


def df_download_thread(
    results_queue: queue.Queue[list[bytes]],
    project_name: str,
    bucket_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    dataflux_download_optimization_params:
    DataFluxDownloadOptimizationParams = None,
    retry_config=MODIFIED_RETRY,
):
    """Threading helper that calls dataflux_download and places results onto queue.

    Args:
        results_queue: the queue on which to put all download results.
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        dataflux_download_optimization_params: the paramemters used to optimize the download performance.
        retry_config: The retry configuration to pass to all retryable download operations
    """
    result = dataflux_download(
        project_name,
        bucket_name,
        objects,
        storage_client,
        dataflux_download_optimization_params,
        # Always signify threading enabled so that signal handling is disabled.
        threading_enabled=True,
        retry_config=retry_config,
    )
    results_queue.put(result)


def dataflux_download_threaded(
    project_name: str,
    bucket_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    dataflux_download_optimization_params:
    DataFluxDownloadOptimizationParams = None,
    threads: int = 1,
    retry_config=MODIFIED_RETRY,
) -> list[bytes]:
    """Perform the DataFlux download algorithm threaded to performantly download the object contents as bytes and return.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        dataflux_download_optimization_params: the paramemters used to optimize the download performance.
        threads: The number of threads on which to download at any given time.
        retry_config: The retry configuration to pass to all retryable download operations
    Returns:
        the contents of the object in bytes.
    """
    chunk_size = math.ceil(len(objects) / threads)
    chunks = []
    for i in range(threads):
        chunk = objects[i * chunk_size:(i + 1) * chunk_size]
        if chunk:
            chunks.append(chunk)
    results_queues = [queue.Queue() for _ in chunks]
    thread_list = []
    for i, chunk in enumerate(chunks):
        thread = threading.Thread(
            target=df_download_thread,
            args=(
                results_queues[i],
                project_name,
                bucket_name,
                chunk,
                storage_client,
                dataflux_download_optimization_params,
                retry_config,
            ),
        )
        thread_list.append(thread)
        thread.start()
    for thread in thread_list:
        thread.join()
    results = []
    for q in results_queues:
        while not q.empty():
            results.extend(q.get())
    return results


def dataflux_download_parallel(
    project_name: str,
    bucket_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    dataflux_download_optimization_params:
    DataFluxDownloadOptimizationParams = None,
    parallelization: int = 1,
    retry_config=MODIFIED_RETRY,
) -> list[bytes]:
    """Perform the DataFlux download algorithm in parallel to download the object contents as bytes and return.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        dataflux_download_optimization_params: the paramemters used to optimize the download performance.
        parallelization: The number of parallel processes that will simultaneously execute the download.
        retry_config: The retry configuration to pass to all retryable download operations
    Returns:
        the contents of the object in bytes.
    """
    chunk_size = math.ceil(len(objects) / parallelization)
    chunks = []
    for i in range(parallelization):
        chunk = objects[i * chunk_size:(i + 1) * chunk_size]
        if chunk:
            chunks.append(chunk)
    with multiprocessing.Pool(processes=len(chunks)) as pool:
        results = pool.starmap(
            dataflux_download,
            ((
                project_name,
                bucket_name,
                chunk,
                storage_client,
                dataflux_download_optimization_params,
                False,
                retry_config,
            ) for chunk in chunks),
        )
        return list(itertools.chain.from_iterable(results))


def dataflux_download(
    project_name: str,
    bucket_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    dataflux_download_optimization_params:
    DataFluxDownloadOptimizationParams = None,
    threading_enabled=False,
    retry_config=MODIFIED_RETRY,
) -> list[bytes]:
    """Perform the DataFlux download algorithm to download the object contents as bytes and return.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        dataflux_download_optimization_params: the paramemters used to optimize the download performance.
        retry_config: The retry configuration to pass to all retryable download operations
    Returns:
        the contents of the object in bytes.
    """
    if storage_client is None:
        storage_client = storage.Client(project=project_name)
    user_agent.add_dataflux_user_agent(storage_client)

    res = []
    max_composite_object_size = (
        dataflux_download_optimization_params.max_composite_object_size)

    i = 0
    # Register the cleanup signal handler for SIGINT.
    if not threading_enabled:
        signal.signal(signal.SIGINT, term_signal_handler)
    global current_composed_object
    while i < len(objects):
        curr_object_name = objects[i][0]
        curr_object_size = objects[i][1]

        if curr_object_size > max_composite_object_size:
            # Download the single object.
            curr_object_content = download_single(
                storage_client=storage_client,
                bucket_name=bucket_name,
                object_name=curr_object_name,
                retry_config=retry_config,
            )
            res.append(curr_object_content)
            i += 1
        else:
            # Dynamically compose and decompose based on the object size.
            objects_slice = []
            curr_size = 0

            while (i < len(objects) and curr_size <= max_composite_object_size
                   and len(objects_slice) < MAX_NUM_OBJECTS_TO_COMPOSE):
                curr_size += objects[i][1]
                objects_slice.append(objects[i])
                i += 1

            if len(objects_slice) == 1:
                object_name = objects_slice[0][0]
                curr_object_content = download_single(
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    retry_config=retry_config,
                )
                res.append(curr_object_content)
            else:
                # If the number of objects > 1, we want to compose, download, decompose and delete the composite object.
                # Need to create a unique composite name to avoid mutation on the same object among processes.
                composed_object_name = COMPOSED_PREFIX + str(uuid.uuid4())
                composed_object = compose(
                    project_name,
                    bucket_name,
                    composed_object_name,
                    objects_slice,
                    storage_client,
                    retry_config=retry_config,
                )
                current_composed_object = composed_object
                res.extend(
                    decompose(
                        project_name,
                        bucket_name,
                        composed_object_name,
                        objects_slice,
                        storage_client,
                        retry_config=retry_config,
                    ))

                try:
                    composed_object.delete(retry=retry_config)
                    current_composed_object = None
                except Exception as e:
                    logging.exception(
                        f"exception while deleting the composite object: {e}")
    return res


def dataflux_download_lazy(
    project_name: str,
    bucket_name: str,
    objects: list[tuple[str, int]],
    storage_client: object = None,
    dataflux_download_optimization_params:
    DataFluxDownloadOptimizationParams = None,
    threading_enabled=False,
    retry_config: "google.api_core.retry.retry_unary.Retry" = MODIFIED_RETRY,
) -> Iterator[bytes]:
    """Perform the DataFlux download algorithm to download the object contents as bytes in a lazy fashion.

    Args:
        project_name: the name of the GCP project.
        bucket_name: the name of the GCS bucket that holds the objects to compose.
            The function uploads the the composed object to this bucket too.
        objects: A list of tuples which indicate the object names and sizes (in bytes) in the bucket.
            Example: [("object_name_A", 1000), ("object_name_B", 2000)]
        storage_client: the google.cloud.storage.Client initialized with the project.
            If not defined, the function will initialize the client with the project_name.
        dataflux_download_optimization_params: the paramemters used to optimize the download performance.
        retry_config: The retry parameter to supply to the compose objects call.
    Returns:
        An iterator of the contents of the object in bytes.
    """
    if storage_client is None:
        storage_client = storage.Client(project=project_name)
    user_agent.add_dataflux_user_agent(storage_client)

    max_composite_object_size = (
        dataflux_download_optimization_params.max_composite_object_size)

    i = 0
    # Register the cleanup signal handler for SIGINT.
    if not threading_enabled:
        signal.signal(signal.SIGINT, term_signal_handler)
    global current_composed_object
    while i < len(objects):
        curr_object_name = objects[i][0]
        curr_object_size = objects[i][1]

        if curr_object_size > max_composite_object_size:
            # Download the single object.
            curr_object_content = download_single(
                storage_client=storage_client,
                bucket_name=bucket_name,
                object_name=curr_object_name,
                retry_config=retry_config,
            )
            yield from [curr_object_content]
            i += 1
        else:
            # Dynamically compose and decompose based on the object size.
            objects_slice = []
            curr_size = 0

            while (i < len(objects) and curr_size <= max_composite_object_size
                   and len(objects_slice) < MAX_NUM_OBJECTS_TO_COMPOSE):
                curr_size += objects[i][1]
                objects_slice.append(objects[i])
                i += 1

            if len(objects_slice) == 1:
                object_name = objects_slice[0][0]
                curr_object_content = download_single(
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    retry_config=retry_config,
                )
                yield from [curr_object_content]
            else:
                # If the number of objects > 1, we want to compose, download, decompose and delete the composite object.
                # Need to create a unique composite name to avoid mutation on the same object among processes.
                composed_object_name = COMPOSED_PREFIX + str(uuid.uuid4())
                composed_object = compose(
                    project_name,
                    bucket_name,
                    composed_object_name,
                    objects_slice,
                    storage_client,
                    retry_config=retry_config,
                )
                current_composed_object = composed_object
                yield from (decompose(
                    project_name,
                    bucket_name,
                    composed_object_name,
                    objects_slice,
                    storage_client,
                    retry_config=retry_config,
                ))

                try:
                    composed_object.delete(retry=retry_config)
                    current_composed_object = None
                except Exception as e:
                    logging.exception(
                        f"exception while deleting the composite object: {e}")


def clean_composed_object(composed_object):
    if composed_object:
        try:
            composed_object.delete(retry=MODIFIED_RETRY)
        except Exception as e:
            logging.exception(
                f"exception while deleting composite object: {e}")


def term_signal_handler(signal_num, frame):
    print("Ctrl+C interrupt detected. Cleaning up and exiting...")
    clean_composed_object(current_composed_object)
    sys.exit(0)
