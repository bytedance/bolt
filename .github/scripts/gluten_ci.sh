#!/usr/bin/env bash
# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

# Shared Spark runtime and report helpers for scripts/gluten-ci/run-task.sh.
set -euo pipefail

BOLT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GLUTEN_DIR="${GLUTEN_DIR:-${BOLT_ROOT}/gluten}"
GLUTEN_REPORTS_DIR="${GLUTEN_REPORTS_DIR:-${BOLT_ROOT}/.ci/gluten-bolt}"

ci_run() {
  local log="$1"
  shift
  mkdir -p "$(dirname "${log}")" || return
  printf '%q ' "$@" | tee -a "${log}" || return
  printf '\n' | tee -a "${log}" || return
  "$@" 2>&1 | tee -a "${log}"
}

ci_runtime() (
  export LD_LIBRARY_PATH="${GLUTEN_DIR}/cpp/build/package/bolt/linux/amd64:${BOLT_ROOT}/.ci/gluten-runtime${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  if [[ -f "${GLUTEN_DIR}/cpp/build/Release/generators/conanrun.sh" ]]; then
    source "${GLUTEN_DIR}/cpp/build/Release/generators/conanrun.sh" || return
  fi
  "$@"
)

ci_spark_environment() {
  BOLT_GLUTEN_SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME:-/opt/shims/${SPARK_RESOURCE_MODULE}/spark_home}"
  test -x "${BOLT_GLUTEN_SPARK_HOME}/bin/spark-submit"
  test -f "${BOLT_GLUTEN_SPARK_HOME}/jars/spark-core_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  test -d "${BOLT_GLUTEN_SPARK_HOME}/sql/core/src/test/resources"
  export SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME}"
  local archives=("${SPARK_HOME}"/python/lib/*.zip)
  test -f "${archives[0]}"
  local zipped
  zipped="$(
    IFS=:
    printf '%s' "${archives[*]}"
  )"
  export PYTHONPATH="${zipped}${PYTHONPATH:+:${PYTHONPATH}}"
}

# Spark archives are large. Cache verified resources per installer revision and
# Spark/Scala version, then copy them so test suites cannot mutate the seed.
ci_spark_resources() (
  local installer="${GLUTEN_DIR}/.github/workflows/util/install-spark-resources.sh"
  local key cache temporary
  key="$({
    sha256sum "${installer}"
    printf '%s\n' "${SPARK_VERSION}/${SCALA_VERSION}/${SPARK_RESOURCE_PROFILE}"
  } | sha256sum | cut -d ' ' -f1)"
  cache="${GLUTEN_CACHE_ROOT:-/data/gluten-ci-cache}/spark/${key}"
  mkdir -p "${cache%/*}"
  exec 7> "${cache}.lock"
  flock --exclusive 7
  if [[ ! -f "${cache}/complete" ]]; then
    temporary="$(mktemp -d "${cache}.XXXXXX")"
    trap 'rm -rf "${temporary}"' EXIT
    bash "${installer}" "${SPARK_RESOURCE_PROFILE}" "${temporary}"
    touch "${temporary}/complete"
    mv "${temporary}" "${cache}"
  fi
  mkdir -p "${BOLT_ROOT}/.ci/spark-resources"
  cp -a --reflink=auto "${cache}/." "${BOLT_ROOT}/.ci/spark-resources/"
)

ci_maven_test() (
  local stage="$1" module="$2"
  shift 2
  local output="${GLUTEN_REPORTS_DIR}/${stage}${CI_GROUP:+/${CI_GROUP}}"
  mkdir -p "${output}" || return
  local log="${output}/run.log" status=0
  : > "${log}" || return
  local command=("$@" -Dmaven.test.failure.ignore=false)
  # The common reactor includes POM-only modules without ScalaTest dependencies.
  # Use their declared test lifecycle instead of invoking every plugin directly.
  local argument common_command=()
  for argument in "${command[@]}"; do
    [[ "${argument}" != surefire:test && "${argument}" != scalatest:test ]] || continue
    common_command+=("${argument}")
  done
  common_command+=(test)
  [[ -z "${CI_SUITES:-}" ]] || command+=("-Dsuites=${CI_SUITES}")
  # Reports must come from this invocation, including when Maven fails.
  shopt -s globstar nullglob
  rm -f "${output}"/**/TEST-*.xml || return
  local reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  if [[ "${module}" == reactor ]]; then
    reports=("${GLUTEN_DIR}"/**/target/*reports/TEST-*.xml)
  fi
  rm -f "${reports[@]}" || return
  cd "${GLUTEN_DIR}" || return
  if [[ "${module}" == reactor ]]; then
    # The preceding install supplies the backend and its active profile
    # dependencies. Select SQL independently so backend suites run only once.
    ci_run "${log}" ci_runtime "${common_command[@]}" \
      -pl "!backends-bolt,!gluten-ut/${SPARK_MODULE}" || status=$?
    ci_run "${log}" ci_runtime "${command[@]}" \
      -pl "gluten-ut/${SPARK_MODULE}" || status=$?
  elif [[ "${module}" == gluten-ut/* ]]; then
    ci_run "${log}" ci_runtime "${command[@]}" -pl "${module}" || status=$?
  else
    ci_run "${log}" ci_runtime "${command[@]}" -pl "${module}" || status=$?
  fi
  if grep -Fq 'Cannot use org.apache.gluten.extension.GlutenSessionExtensions' "${log}"; then
    echo "Gluten session extension failed to initialize" | tee -a "${log}" >&2
    status=1
  fi
  local path suite relative executed=0 count
  reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  if [[ "${module}" == reactor ]]; then
    reports=("${GLUTEN_DIR}"/**/target/*reports/TEST-*.xml)
  fi
  for path in "${reports[@]}"; do
    relative="${path#"${GLUTEN_DIR}/"}"
    mkdir -p "${output}/${relative%/*}" || return
    cp "${path}" "${output}/${relative}" || return
    # A helper-module test cannot hide an empty Spark SQL group.
    if [[ "${module}" == reactor && "${relative}" != "gluten-ut/${SPARK_MODULE}/target/"* ]]; then
      continue
    fi
    if [[ "${status}" == 0 ]]; then
      count=$(xmllint --nonet --xpath 'count(//testcase[not(skipped)])' "${path}") || return
      # Unrelated Surefire tests must not hide an empty ScalaTest selection.
      suite="${path##*/TEST-}"
      if [[ -n "${CI_SUITES:-}" && ",${CI_SUITES}," != *",${suite%.xml},"* ]]; then
        continue
      fi
      executed=$((executed + count))
    fi
  done
  if [[ "${status}" == 0 && "${executed}" == 0 ]]; then
    echo "No executed tests in fresh reports for ${stage}/${module}" | tee -a "${log}" >&2
    return 1
  fi
  return "${status}"
)
