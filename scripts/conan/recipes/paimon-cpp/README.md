# Paimon C++ Conan recipe

Versions beginning with `v` use checksum-verified release archives from
`apache/paimon-cpp`. Versions without that prefix retain the Git URL/revision
source configuration. `config.yml` and `all/conandata.yml` list supported
versions and their version-specific patches.

## Apache v0.3.0

Build the static Release package used by Bolt:

```bash
conan create scripts/conan/recipes/paimon-cpp/all --version=v0.3.0 \
  -s build_type=Release -o 'paimon-cpp/*:shared=False' \
  -o 'paimon-cpp/*:with_avro=True' -o 'paimon-cpp/*:with_orc=False' \
  -c tools.build:jobs=24 --build='paimon-cpp/*'
```

The recipe uses the existing Bolt dependency versions, including Arrow
`15.0.1-oss`, fmt 9 and glog 0.7.1. Its release-specific patch connects Conan
dependencies, packages internal archives, adapts Arrow memory accounting and
fmt includes, and works around a GCC optional-value warning.

`with_parquet=False` is the default for Apache releases. Their native Parquet
plugin requires private APIs from upstream's patched Arrow, which are absent
from this recipe's Arrow 15 dependency. Enabling that option produces an early
configuration error. Consumers supply a Parquet format implementation, as Bolt
does; `Paimon::format_parquet` is not exported when the plugin is disabled.
The legacy `0.0.4-bolt` recipe retains its native Parquet plugin and options.
`with_lance=True` is also rejected for Apache v0.3.0, which has no Lance plugin.

The v0.3.0 core exports `FileBatchReader::GetPreviousBatchFileRowId(index)`,
which lets readers report physical positions for compacted filter results.
The Conan consumer test exercises the installed interface and the core's default
batch bitmap implementation, and checks the Arrow bridge's allocation counters
through allocation, growth, unchanged-size reallocation, shrinkage and free.
This is a package/linkage smoke test. A Bolt integration requires reader,
read-context and signed 64-bit filesystem API adaptations. Compact Parquet
predicate/bitmap results need a physical row ID for every returned row.
Arrow exports must flatten constant and dictionary vectors when Paimon's row
merge expects primitive buffers. Consumers providing their own ORC reader do not need to enable Paimon's
`with_orc` option.

The recipe includes neither the internal chain-split nor range-partition patch.
