#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Source the reference test selectors and report handling. The reference ci_init
# supports only Spark 3.5; select the Maven profile and resources for this job.
# shellcheck source=/dev/null
source "${GLUTEN_CI_DRIVER_DIR:-bolt-ci-driver}/.github/scripts/gluten_ci.sh"

case "${SPARK_PROFILE}" in
  3.5)
    SCALA_VERSION=2.12
    connectors=(-Piceberg -Pdelta -Phudi -Ppaimon)
    ;;
  4.0)
    SCALA_VERSION=2.13
    connectors=(-Pdelta -Ppaimon)
    ;;
  4.1 | 4.2)
    SCALA_VERSION=2.13
    connectors=(-Pdelta)
    ;;
  *)
    echo "Unsupported Spark profile: ${SPARK_PROFILE}" >&2
    return 1
    ;;
esac
SPARK_MODULE="spark${SPARK_PROFILE/./}"
if [[ "${GLUTEN_CI_SCALA_VERSION:-$SCALA_VERSION}" != "$SCALA_VERSION" ]]; then
  if [[ "$SPARK_PROFILE" != 3.5 || "$GLUTEN_CI_SCALA_VERSION" != 2.13 ]]; then
    echo "Unsupported Spark / Scala combination: $SPARK_PROFILE / $GLUTEN_CI_SCALA_VERSION" >&2
    return 1
  fi
  SCALA_VERSION=2.13
  connectors=(-Piceberg -Pdelta)
fi
# Consumed by the driver functions and the calling workflow.
# shellcheck disable=SC2034
SPARK_RESOURCE_MODULE="$SPARK_MODULE"
# shellcheck disable=SC2034
SPARK_RESOURCE_PROFILE="$SPARK_PROFILE"
if [[ "$SPARK_PROFILE/$SCALA_VERSION" == 3.5/2.13 ]]; then
  SPARK_RESOURCE_MODULE+="-scala-2.13"
  SPARK_RESOURCE_PROFILE+="-scala2.13"
fi
SPARK_VERSION=$(xmllint --nonet --xpath \
  "string(//*[local-name()='profile'][*[local-name()='id']='spark-${SPARK_PROFILE}']/*[local-name()='properties']/*[local-name()='spark.version'])" \
  "$GLUTEN_DIR/pom.xml")
test -n "$SPARK_VERSION"
JDK_VERSION=17
mkdir -p "$GLUTEN_REPORTS_DIR"
export SPARK_TESTING=true SPARK_SCALA_VERSION="$SCALA_VERSION" SPARK_ANSI_SQL_MODE=false
# shellcheck disable=SC2034
MVN=("$GLUTEN_DIR/build/mvn" -ntp -fae -Pfast-build -Pbackends-bolt "-Pspark-$SPARK_PROFILE"
  "-Pscala-$SCALA_VERSION" "-Pjava-$JDK_VERSION" "-Dmaven.compiler.release=$JDK_VERSION"
  -DskipTests=false -Dmaven.test.skip=false -Dmaven.test.failure.ignore=false
  -DfailIfNoTests=false "${connectors[@]}")
