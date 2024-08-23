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

Fake GCS package supporting the GCS API methods used for Dataflux.

The fake_gcs package provides Client, Bucket, and GCSObject classes matching the
interfaces used in Dataflux code. The fake is implemented using these classes,
rather than by using an HTTP server and connecting the actual GCS client to the
server, which could be a future improvement.
"""

from __future__ import annotations

import io

from google.cloud.storage import _http


class Bucket(object):
    """Bucket represents a bucket in GCS, containing objects."""

    def __init__(self, name: str):
        if not name:
            raise Exception("bucket name must not be empty")
        self.name = name
        self.blobs: dict[str, Blob] = dict()
        self.permissions: any = []

    def list_blobs(
        self,
        max_results: int = 0,
        start_offset: str = "",
        end_offset: str = "",
        prefix: str = "",
        retry: "google.api_core.retry.retry_unary.Retry" = None,
    ) -> list[Blob]:
        results = []
        for name in sorted(self.blobs):
            if max_results and len(results) == max_results:
                break
            if (not start_offset or name
                    >= start_offset) and (not end_offset or name < end_offset):
                if name.startswith(prefix):
                    results.append(self.blobs[name])
        return results

    def blob(self, name: str):
        missing_path = False
        if name == "missing-path":
            missing_path = True
        if name not in self.blobs:
            self.blobs[name] = Blob(
                name, bucket=self, missing_bucket=missing_path)
        return self.blobs[name]

    def _add_file(self,
                  filename: str,
                  content: bytes,
                  storage_class="STANDARD"):
        self.blobs[filename] = Blob(filename,
                                    content,
                                    self,
                                    storage_class=storage_class)

    def test_iam_permissions(self, permissions: any):
        return [p for p in permissions if p in self.permissions]


class FakeBlobWriter(object):
    """Represents fake BlobWriter."""

    def __init__(self, blob):
        self.blob = blob

    def write(self, data: bytes):
        self.blob.content += data

    def flush(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Blob(object):
    """Blob represents a GCS blob object.

    Attributes:
        name: The name of the blob.
        retry: A variable tracking the retry policy input.
        content: The byte content of the Blob.
        bucket: The bucket object in which this Blob resides.
        size: The size in bytes of the Blob.
    """

    def __init__(
        self,
        name: str,
        content: bytes = b"",
        bucket: Bucket = None,
        storage_class="STANDARD",
        missing_bucket: bool = False
    ):
        self.name = name
        self.retry = None
        self.content = content
        self.bucket = bucket
        self.size = len(self.content)
        self.storage_class = storage_class
        self.missing_bucket = missing_bucket

    def compose(self, sources: list[str], retry=None):
        b = b""
        for item in sources:
            b += self.bucket.blobs[item.name].content
        self.content = b
        self.retry = retry

    def delete(self, retry=None):
        del self.bucket.blobs[self.name]

    def exists(self, retry=None):
        return not self.missing_bucket

    def download_as_bytes(self, retry=None):
        return self.content

    def open(self, mode: str, ignore_flush: bool = False):
        if mode == "rb":
            return io.BytesIO(self.content)
        elif mode == "wb":
            self.content = b""
            return FakeBlobWriter(self)
        raise NotImplementedError(
            "Supported modes strings are 'rb' and 'wb' only.")


class Client(object):
    """Client represents a GCS client which can provide bucket handles."""

    def __init__(self):
        self.buckets: dict[str, Bucket] = dict()
        self.content: dict[str, tuple[str, str]] = dict()
        self._connection = _http.Connection(self)

    def bucket(self, name: str) -> Bucket:
        if name not in self.buckets:
            self.buckets[name] = Bucket(name)
            if name in self.content:
                self.buckets[name].content = self.content[name]
        return self.buckets[name]

    def _set_perm(self, permissions: any, name: str):
        self.buckets[name].permissions = permissions
