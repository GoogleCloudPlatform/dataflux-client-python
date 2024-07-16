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

from google.api_core.client_info import ClientInfo
from google.cloud import storage

user_agent_string = "dataflux/1.0"


def add_dataflux_user_agent(storage_client: storage.Client):
    if not storage_client._connection:
        return
    if not storage_client._connection._client_info:
        storage_client._connection._client_info = ClientInfo(
            user_agent=user_agent_string)
    elif not storage_client._connection._client_info.user_agent:
        storage_client._connection._client_info.user_agent = user_agent_string
    elif user_agent_string not in storage_client._connection._client_info.user_agent:
        storage_client._connection._client_info.user_agent = user_agent_string + \
            " " + storage_client._connection._client_info.user_agent
