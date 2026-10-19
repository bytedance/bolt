<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.
SPDX-License-Identifier: Apache-2.0
-->

# Hive Metastore protocol definitions

This directory records the sources and notices for the Apache IDLs used to
generate Hive Metastore bindings. See the
[build and IDE instructions](../../../packaging/BUILDING.md#hive-metastore-bindings)
for downloads, caching, and editable development.

| IDL | Upstream revision | Upstream SHA-256 |
| --- | --- | --- |
| `hive_metastore.thrift` | Apache Hive 2.3.9, `92dd0159f440ca7863be3232f3a683a510a62b9d` | `6e35b250523ad17ff62de2f38ca39f73d8a9b4659003d4e0295b2719e2942ca2` |
| `fb303.thrift` | Apache Thrift 0.9.3, `53dd39833a08ce33582e5ff31fa18bb4735d6731` | `925771d4a9a4288ec95926ccc3f7a8feab68368171ae9efe34bacf21703f8547` |

Sources:

- [Hive IDL](https://raw.githubusercontent.com/apache/hive/92dd0159f440ca7863be3232f3a683a510a62b9d/metastore/if/hive_metastore.thrift)
- [fb303 IDL](https://raw.githubusercontent.com/apache/thrift/53dd39833a08ce33582e5ff31fa18bb4735d6731/contrib/fb303/if/fb303.thrift)

CMake adds only an explicit Python namespace to a copy of each downloaded IDL
so Thrift applies `package_prefix` to cross-module imports. This does not alter
the wire schema.

Hive 2.3.9 matches the previously checked-in bindings:
154 HMS RPCs, struct fields, defaults, and enums. This preserves that protocol
baseline; it does not establish compatibility with every Hive server version.
The newer compiler correctly exposes IDL `binary` fields as Python `bytes`;
the old compiler used string methods for these fields. Bolt's current table and
partition reads do not use those binary fields.

Both IDLs carry Apache-2.0 headers. The full license is in the repository and
sdist root `LICENSE`, and in the wheel's `dist-info/licenses/LICENSE`.
`NOTICE.txt` retains the applicable Hive and Thrift attribution. The Hive
distribution's JSON dependency notice is omitted because no JSON library is
copied here.

To update the protocol, change the fixed upstream URLs and hashes in
`HiveThrift.cmake`, update this provenance and applicable notices, and test the
supported HMS operations against the intended server.
