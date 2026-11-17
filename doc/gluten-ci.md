# Gluten CI

Bolt PRs compile Apache Gluten main with the candidate Bolt package and run native
UT. Daily Spark UT runs at 02:00 Asia/Shanghai with Bolt main and Apache Gluten main.

- [Workflow, coverage and validation](../scripts/gluten-ci/README.md)
- [Arrow source cache deployment](../.github/runners/arrow-cache.md)

All workflows, task helpers and Spark drivers are maintained in this repository.
PR checks use the same candidate commit for Bolt, task helpers and Spark drivers.
Nightly uses the Bolt main commit for all three.
