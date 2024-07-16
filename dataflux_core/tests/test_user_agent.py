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

from google.api_core.client_info import ClientInfo
from google.cloud import storage

from dataflux_core import user_agent


class UserAgentTest(unittest.TestCase):

    def test_no_existing_info(self):
        client = storage.Client()
        user_agent.add_dataflux_user_agent(client)
        self.assertTrue(client._connection.user_agent.startswith("dataflux"))

    def test_no_existing_string(self):
        client = storage.Client(client_info=ClientInfo())
        user_agent.add_dataflux_user_agent(client)
        self.assertTrue(client._connection.user_agent.startswith("dataflux"))

    def test_with_existing_string(self):
        existing_user_agent = "existing user agent"
        client = storage.Client(client_info=ClientInfo(
            user_agent=existing_user_agent))
        user_agent.add_dataflux_user_agent(client)
        self.assertTrue(client._connection.user_agent.startswith("dataflux"))
        self.assertIn(existing_user_agent,
                      client._connection._client_info.user_agent)
