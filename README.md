<img src="static/logo.png" alt="Bolt logo" width="100%" align="center" />

Bolt is a C++ acceleration library providing composable, extensible and performant data processing toolkit. It is designed to provide generic and unified interfaces which can be pluggable into "any framework" running on "any hardware" to consume "any data source".

Initially derived from [Velox](https://github.com/facebookincubator/velox) project, Bolt is created by ByteDance to embrace and unify the contributions from the community. It has been validated on Spark/Flink/Presto/ElasticSearch framework running on x64&ARM CPU/DPU/GPU accessing Parquet/ORC/Text/CSV/Lance file format managed under Hive/Paimon table to provide enterprise-grade cost optimization, results consistency and feature parity

## Why Bolt?

### “Open Source-First” Philosophy

"Contributions may come in many forms, and all of them are valuable". The governance model of Bolt community will be in line with the [Apache Way](https://www.apache.org/theapacheway/) and "Community over Code" spirit. While we are working out the detailed governance model on a tree-tier structure of Contributor / Maintainers / Project Management Committee(PMC), we are committed to treating the open source repository as the source of truth, including but not limited to
* Public CI pipelines
* Clear dependency management as code
* Equal code review opportunity for manintainers
* Transparent design discussion

This will ensure the smooth & credible experience for code contribution

### Embrace the Analytical Ecosystem

Bolt focuses on the physical execution layer of DBMS while providing first-class and high performance support for popular frameworks and storage formats.

Frameworks:
* [Apache Gluten](https://github.com/apache/gluten#bolt-backend) for Apache Spark
* RFC for [PrestoDB](https://github.com/prestodb/rfcs/pull/59)  
* [OpenSearch](https://github.com/opensearch-project/sql/issues/4812?open_in_browser=true) for ElasticSearch
* Flink (Coming Soon)
* ...

Storage Formats:
* Parquet
* ORC
* TXT
* CSV
* Paimon
* Lance (Coming Soon)
* ...

### Enterprise-Grade Performance, Result Consistency & Feature parity

Bolt is designed as a seamless acceleration layer that requires minimum code changes to the existing user jobs. Results/Performance comparison against original frameworks is performed on regular basis to capture regression & corner cases. Key features including
* Adaptive Task Parallelism
* Native Memory Management & Dynamic offheap threshold
* Operator Fusion
* JIT for hotspot expression
* Native Shuffle Support
* ...

## Getting Started

### Get the Bolt Source
```shell
git clone https://github.com/bytedance/bolt.git
cd bolt
```

### Setup Develop Env
We provide scripts to help developers configure the environment and install dependencies.
```shell
scripts/setup-dev-env.sh
```

Bolt uses [Conan](https://conan.io/) as its dependency management tool, which is an open source and multi-platform package manager.

This script exports conan recipes to local cache. For the first time, dependencies will be built from source and installed into local cache. You can setup your own [conan server](https://docs.conan.io/2/reference/conan_server.html#conan-server) to accelerate building.

### Building Bolt
#### Building Bolt for Presto

Run `make` in the root directory to compile the sources. For development, use
`make debug` to build a non-optimized debug version, or `make release` to build
an optimized version.  Use `make unittest` to build and run tests.

```shell
make release

# In main branch, by default, BUILD_VERSION is main.
make release BUILD_VERSION=main
```

#### Building Bolt as an [Apache Gluten](https://gluten.apache.org/) Backend

Bolt is now available as a backend in Apache Gluten. The integration was merged via [apache/gluten#12454](https://github.com/apache/gluten/pull/12454).

For the latest prerequisites and standard build instructions, see the [Bolt Backend](https://github.com/apache/gluten#bolt-backend) section in the Apache Gluten repository.

To validate local Bolt changes with Gluten, first build Bolt in Spark-compatible mode and export it to the local Conan cache:

```shell
# In the Bolt repository
make release_spark
make export_release
```

Then clone the official Apache Gluten repository and build the Bolt backend and Gluten JAR:

```shell
git clone https://github.com/apache/gluten.git
cd gluten

make release
make arrow

# Select the target matching your Spark version.
# For example, build the Spark 3.5 package:
make jar_spark35
```

#### Building Bolt for other system
You can use the `make release && make export_release` command to compile and export Bolt, and then use conan to reference Bolt. Below is a conanfile example that references Bolt as a third-party dependency.

```python
# Take gluten for example:
class GluenConan(ConanFile):
  def requirements(self):
    bolt_version="main"
    self.requires(f"bolt/{bolt_version}", transitive_headers=True, transitive_libs=True)
```


## Contributing

Check our [contributing guide](CONTRIBUTING.md) to learn about how to
contribute to the project.

## Community

* [Please join our Slack channel and ask in `#dev`](https://join.slack.com/t/bolt-lib/shared_invite/zt-3okvtb6fr-7jUlaI2AiGCOYdoxhNL6Pw).

## License

Bolt is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)
