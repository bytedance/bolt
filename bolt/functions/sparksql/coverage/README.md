# Coverage Maps

Bolt includes a subset of scalar and aggregate Spark functions.
bolt_sparksql_coverage utility is used to generate a list of available
functions. The output uses reStructured text format suitable for use in
Bolt documentation.

> bolt_sparksql_coverage

Generates a list of Spark functions available in Bolt. The output
to be copy-pasted into bolt/docs/spark_functions.rst file.

> bolt_sparksql_coverage --bolt_spark_coverage_all

Generates coverage map using all Spark functions. The output to be copy-pasted
into bolt/docs/functions/spark/coverage.rst file. The functions appear in alphabetical order.

The list of all scalar and aggregate Spark functions comes from
data/all_scalar_functions.txt and data/all_aggregate_functions.txt files respectively.
Therefore, make sure your current working directory is bolt/functions/sparksql/coverage
before running the executable so that these files are picked up correctly.

These files were created using output of SHOW FUNCTIONS Spark command.
