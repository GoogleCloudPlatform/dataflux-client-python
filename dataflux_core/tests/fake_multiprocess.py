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

Fake GCS package supporting the GCS API methods used for Dataflux.

The fake_gcs package provides Client, Bucket, and GCSObject classes matching the
interfaces used in Dataflux code. The fake is implemented using these classes,
rather than by using an HTTP server and connecting the actual GCS client to the
server, which could be a future improvement.
"""


class FakeProcess(object):
    """A fake multiprocessing process for testing purposes."""

    def __init__(self, name: str, alive: bool = False, term_tracker=[]):
        self.name = name
        self.alive = alive
        self.term_tracker = term_tracker

    def is_alive(self):
        if self.alive:
            self.alive = False
            return True
        return self.alive

    def terminate(self):
        self.term_tracker.append("")
