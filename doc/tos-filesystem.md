# TOS filesystem

Bolt can access `tos://bucket/object` through the native Volcengine TOS C++ SDK.
The adapter is optional and uses `tos_client/2.6.28` from the
[ByteDance Conan recipes](https://github.com/bytedance/conan-center-index/tree/main/recipes/tos_client/all).

## Build and registration

With the Conan environment described in [CONTRIBUTING.md](../CONTRIBUTING.md):

```sh
make release_with_test CONAN_OVERRIDE='-o bolt/*:enable_tos=True'
```

If the configured Conan remote does not yet contain version 2.6.28, first
export it from a checkout of the linked ByteDance recipe repository:

```sh
conan export /path/to/conan-center-index/recipes/tos_client/all --version=2.6.28
```

The Conan option `enable_tos` sets CMake's `BOLT_ENABLE_TOS`; both default to off.
A direct CMake build needs the `tos_client::tos_client` package target when
TOS is enabled. Disabled builds do not require the SDK.

Hive connector initialization registers the adapter automatically. Applications
using the filesystem API directly can register it explicitly:

```cpp
#include "bolt/common/config/Config.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/RegisterTosFileSystem.h"

using namespace bytedance::bolt;

filesystems::registerTosFileSystem();
auto properties = std::make_shared<config::ConfigBase>(
    std::unordered_map<std::string, std::string>{
        {"tos.endpoint", "https://tos-cn-beijing.volces.com"},
        {"tos.region", "cn-beijing"},
        {"tos.credentials.file.path", "/path/to/credentials.xml"}});
auto fs = filesystems::getFileSystem("tos://example-bucket/data.parquet", properties);
auto file = fs->openFileForRead("tos://example-bucket/data.parquet");
```

Registration is idempotent and is a no-op when TOS is disabled. Each open file
owns its SDK client. SDK initialization is performed once; the adapter does not
call process-wide `CloseClient()` when a file is destroyed, since other
components may still use TOS or libcurl.

## Configuration

These properties can be supplied through connector properties or
`FileOptions::values`:

| Property | Meaning |
| --- | --- |
| `tos.endpoint` | Native TOS service hostname, with an optional HTTP/HTTPS scheme. IP addresses, explicit ports and S3 endpoints are unsupported by the SDK. |
| `tos.region` | Signing region. If absent, inferred from a `tos-REGION.*` endpoint. Set explicitly for custom endpoints. |
| `tos.access.key` | Access key ID. |
| `tos.secret.key` | Secret access key; must be supplied together with the access key. |
| `tos.session.token` | Optional token for temporary credentials. |
| `tos.credentials.file.path` | Hadoop XML credential file, read on each file open. |

Each property also accepts the `fs.tos.` prefix. Bucket overrides use
`tos.bucket.BUCKET.PROPERTY` or `fs.tos.bucket.BUCKET.PROPERTY`, for example
`tos.bucket.example-bucket.endpoint`.

For each property, file options take precedence over connector properties.
Within either source, bucket properties precede base properties, and `tos.`
precedes `fs.tos.` at the same specificity. `bolt.tos.endpoint` and
`hive.tos.endpoint` are fallback endpoint aliases.

A configured credential file takes precedence over direct access/secret keys.
A credential file path in file options takes precedence over one in connector
properties. The XML properties are:

- `fs.tos.access-key-id`
- `fs.tos.secret-access-key`
- `fs.tos.session-token` (optional)

Bucket credentials use `fs.tos.bucket.BUCKET.` with the same suffixes. A matched
bucket must provide its own complete access/secret pair; it does not inherit
keys or tokens from the base credentials. Credentials are resolved for each
open so file rotation does not require rebuilding the filesystem instance.

## Operations

Reads support positional reads and vector reads with gaps. Vector reads fetch
one contiguous range and copy the requested slices. Zero-length reads do not
issue object GET requests. Reads outside the object size fail.

Writes follow the reference adapter's bucket semantics: FNS uses AppendObject;
HNS uses PutObject for the first non-empty append and AppendObject thereafter.
SDK 2.6.28 translates HNS AppendObject calls to ModifyObject requests.
Closing a file with no data creates an empty object. Repeated close is harmless;
append after close fails. Each append is synchronous, and `flush()` has no
additional work. These writes do not provide atomic publication on close.

`FileOptions::shouldThrowOnFileAlreadyExists` defaults to true. When overwrite
is requested, FNS removes an existing object before appending from offset zero;
HNS overwrites on its initial PutObject. The existence check is not an atomic
exclusive-create operation across concurrent writers. Only a server-side 404
means absence; permission and transport failures are propagated.

Bucket creation requires `shouldCreateParentDirectories=true`. The adapter
leaves `remove`, `rename`, `exists`, `list`, `mkdir` and `rmdir` unsupported,
matching the reference implementation.

The optional `TosFileSystemExtension` interface allows an embedding application
to customize SDK client configuration and receive bucket-access notifications.
One extension can be registered per process. Callback failures are isolated from
file I/O. No bucket-tagging implementation is included in this adapter.

## Tests

After configuring with tests enabled:

```sh
cmake --build --preset conan-release --target bolt_tos_file_system_test bolt_tos_extension_registry_test
ctest --test-dir _build/Release -R '^bolt_tos_(file_system|extension_registry)_test$' --output-on-failure
```

Tests cover configuration precedence, credential rotation, cache isolation,
extension failures, registration, and reads/writes through the real SDK against
a scripted loopback HTTP server. They require no cloud credentials.

## Public usage references

- [JuiceFS TOS object storage](https://github.com/juicedata/juicefs/blob/main/pkg/object/tos.go): Go SDK integration, range reads, credentials, errors and multipart uploads.
- [fsspec tosfs](https://github.com/fsspec/tosfs/blob/main/tosfs/core.py): Python filesystem integration and buffered writes.
- [Apache Hadoop TOS filesystem](https://github.com/apache/hadoop/tree/trunk/hadoop-cloud-storage-project/hadoop-tos): Java filesystem integration.

These projects provide filesystem behavior references. C++ API compatibility is
checked against the native SDK used by the Conan recipe.
