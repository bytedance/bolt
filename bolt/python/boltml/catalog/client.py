# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from .._generated.hive_metastore.ThriftHiveMetastore import Client as ThriftClient


class HiveClient(ThriftClient):
    """
    Client connection to a hive thrift server.
    This implements the ThriftClient interface from ThriftHiveMetastore.
    """

    def __init__(self, hostname: str = "localhost", port: int = 9083):
        socket = TSocket.TSocket(hostname, port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        super().__init__(protocol)
        self._oprot.trans.open()

    def __del__(self) -> None:
        """Close transport connection."""
        self._oprot.trans.close()
