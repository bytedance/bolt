# Apache Gluten prerequisites

[Apache Gluten #13486](https://github.com/apache/gluten/pull/13486) merged the
native build and Spark test source prerequisites in
`3f4439f428bb7c7e399f60754b7ebe28263d0f6b`.

Bolt PR compilation and nightly UT follow `apache/gluten` `main`. The prepare
job resolves the ref once, and every worker checks out that exact commit.
Manual runs may supply an exact commit to reproduce a failure.

Bolt owns the workflows, helpers, matrix and deployment. No Gluten fork or
runtime source patch is required. The nightly matrix contains 39 tasks:
1 native, 12 packaging and 26 Spark/JVM/connector tasks for Spark 3.5, 4.0,
4.1 and 4.2. Spark 3.4 is outside this scope.

Connector test dependencies use explicit `bolt-ut-*` Maven profiles, selected
by Bolt's task helper. Runtime correctness fixes are separate from these
compilation prerequisites; helper tests do not prove native or Spark UT pass.
