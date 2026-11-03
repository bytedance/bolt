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

# Source this file from workflow steps or a local shell. Test selection and
# build commands live in gluten-bolt.yml; this helper preserves their evidence.
set -euo pipefail

BOLT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GLUTEN_DIR="${GLUTEN_DIR:-${BOLT_ROOT}/gluten}"
GLUTEN_REPORTS_DIR="${GLUTEN_REPORTS_DIR:-${BOLT_ROOT}/.ci/gluten-bolt}"

ci_conan_home() {
  # Container jobs have an exclusive /github/home mount. Matching the existing
  # Bolt jobs' dependency paths allows their compiler cache entries to be reused.
  if [[ "${GITHUB_ACTIONS:-}" == true && "${HOME}" == /github/home ]]; then
    printf '%s\n' "${HOME}/.conan2"
  else
    printf '%s\n' "${RUNNER_TEMP:-/tmp}/gluten-ci-conan-$1"
  fi
}

ci_cache_environment() {
  local cache="${GLUTEN_CACHE_ROOT:-/data/gluten-ci-cache}"
  local slot
  slot="$(printf '%s' "${RUNNER_NAME:-local}" | sha256sum | cut -c1-16)"
  local profile="${CI_BUILD:-native}"
  [[ "${profile}" =~ ^[a-z0-9-]+$ ]] || return 1
  # Each runner handles one job at a time. Preserve its package database so
  # rebuilt dependencies retain their paths and compiler cache identities.
  export CONAN_HOME
  CONAN_HOME="$(ci_conan_home "${profile}")"
  local packages="${cache}/conan/${slot}/${profile}"
  mkdir -p "${packages}" "$(dirname "${CONAN_HOME}")"
  if [[ -L "${CONAN_HOME}" ]]; then
    rm -- "${CONAN_HOME}"
  elif [[ -e "${CONAN_HOME}" ]]; then
    rm -rf -- "${CONAN_HOME}"
  fi
  ln -s "${packages}" "${CONAN_HOME}"
  export GLUTEN_MAVEN_REPO="${cache}/maven/${slot}/${CI_BUILD:-native}"
  export PIP_CACHE_DIR="${cache}/pip/${slot}"
  mkdir -p "${CONAN_HOME}" "${GLUTEN_MAVEN_REPO}" "${PIP_CACHE_DIR}"
  mkdir -p "${GLUTEN_REPORTS_DIR}"
  printf 'Bolt %s\nGluten %s\n' "$(git -C "${BOLT_ROOT}" rev-parse HEAD)" \
    "$(git -C "${GLUTEN_DIR}" rev-parse HEAD)" > "${GLUTEN_REPORTS_DIR}/sources.txt"
  mkdir -p "${cache}/maven-tools/${slot}"
  if [[ ! -e "${GLUTEN_DIR}/build/.mvn" ]]; then
    ln -s "${cache}/maven-tools/${slot}" "${GLUTEN_DIR}/build/.mvn"
  fi
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf 'CONAN_HOME=%s\nGLUTEN_MAVEN_REPO=%s\nPIP_CACHE_DIR=%s\n' \
      "${CONAN_HOME}" "${GLUTEN_MAVEN_REPO}" "${PIP_CACHE_DIR}" >> "${GITHUB_ENV}"
  fi
}

ci_seed_conan() {
  local archive="${GLUTEN_CONAN_SEED:-/opt/gluten-ci-seed/conan.tgz}"
  if [[ -f "${archive}" ]]; then
    local digest marker="${CONAN_HOME}/.gluten-ci-seed"
    digest="$(sha256sum "${archive}" | cut -d' ' -f1)"
    if [[ ! -f "${marker}" || "$(cat "${marker}")" != "${digest}" ]]; then
      conan cache restore "${archive}"
      printf '%s\n' "${digest}" > "${marker}"
    fi
  else
    echo "No Conan seed: using the persistent cache and configured remotes"
  fi
  # Candidate archives are unique to a run and must not accumulate in this
  # dependency cache. The current candidate is exported later in this job.
  conan remove 'bolt/ci-*' --confirm
  local downloads="${GLUTEN_MAVEN_REPO%/maven/*}/conan-downloads"
  mkdir -p "${downloads}"
  printf 'core.download:download_cache=%s\n' "${downloads}" >> "${CONAN_HOME}/global.conf"
}

ci_set_maven() {
  MVN=("${GLUTEN_DIR}/build/mvn" -ntp -fae -Pfast-build -Pbackends-bolt
    "-Pspark-${SPARK_PROFILE}" "-Pscala-${SCALA_VERSION}" "-Pjava-${JDK_VERSION}"
    "-Dmaven.compiler.release=${JDK_VERSION}"
    "-DsecondaryCacheDir=${GLUTEN_MAVEN_REPO}/.zinc"
    -DskipTests=false -Dmaven.test.skip=false
    -Dmaven.test.failure.ignore=false -DfailIfNoTests=false)
  MVN+=(-Piceberg -Pdelta -Phudi -Ppaimon)
}

ci_init() {
  GLUTEN_DIR="$(cd "${GLUTEN_DIR}" && pwd)"
  mkdir -p "${GLUTEN_REPORTS_DIR}"
  GLUTEN_REPORTS_DIR="$(cd "${GLUTEN_REPORTS_DIR}" && pwd)"
  GLUTEN_MAVEN_REPO="${GLUTEN_MAVEN_REPO:-${HOME}/.m2/repository}"
  export MAVEN_ARGS="${MAVEN_ARGS:-} -Dmaven.repo.local=${GLUTEN_MAVEN_REPO}"
  SCALA_VERSION=2.12
  [[ "${CI_BUILD:-spark35}" == spark35 ]] || return 1
  SPARK_PROFILE=3.5
  SPARK_MODULE=spark35
  SPARK_VERSION="$(xmllint --nonet --xpath \
    "string(/*[local-name()='project']/*[local-name()='profiles']/*[local-name()='profile'][*[local-name()='id']='spark-${SPARK_PROFILE}']/*[local-name()='properties']/*[local-name()='spark.version'])" \
    "${GLUTEN_DIR}/pom.xml")"
  [[ -n "${SPARK_VERSION}" ]]
  SPARK_RESOURCE_PROFILE="${SPARK_PROFILE}"
  SPARK_RESOURCE_MODULE="${SPARK_MODULE}"
  JDK_VERSION=17
  ci_set_maven
  export SPARK_TESTING=true SPARK_SCALA_VERSION="${SCALA_VERSION}" SPARK_ANSI_SQL_MODE=false
}

ci_conan_options() {
  options=(-o "bolt/*:spark_compatible=True" -o "bolt/*:enable_testutil=True"
    -o "bolt/*:shared=False" -o "bolt/*:enable_hdfs=True"
    -o "bolt/*:enable_s3=False" -o "bolt/*:enable_asan=False")
  gluten_options=(-o "gluten/*:shared=True" -o "gluten/*:build_tests=True"
    -o "gluten/*:enable_hdfs=True" -o "gluten/*:enable_s3=False"
    -o "gluten/*:enable_asan=False")
  profile=(-pr:h default)
}

ci_verify_bolt_package() {
  # Gluten must consume the exact exported Bolt binary, without rebuilding it.
  jq -e --slurpfile exported "${GLUTEN_REPORTS_DIR}/bolt-export.json" '
    [.graph.nodes[] | select(.name == "bolt" and .context == "host")] as $actual |
    [$exported[0].graph.nodes[] | select(.name == "bolt" and .context == "host")] as $expected |
    ($actual | length) == 1 and ($expected | length) == 1 and
    $actual[0].binary == "Cache" and
    ($actual[0] | {ref, package_id, prev}) == ($expected[0] | {ref, package_id, prev})
  ' "${GLUTEN_REPORTS_DIR}/gluten-install.json"
}

# Verify the complete archive before extraction. A stopped artifact stream can
# otherwise leave missing or truncated files while its action reports success.
ci_download_artifact() (
  local metadata url digest archive attempt artifact_id cached
  local cache="${GLUTEN_CACHE_ROOT:-/data/gluten-ci-cache}/artifacts"
  local api="${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts"
  local auth=(-H "Authorization: Bearer ${GITHUB_TOKEN:?}")
  metadata="$(curl -fsSL --retry 3 --retry-all-errors \
    "${auth[@]}" --get --data-urlencode "name=$1" "${api}")" || return
  url="$(jq -er '.artifacts | select(length == 1) | .[0].archive_download_url' <<< "${metadata}")" || return
  digest="$(jq -er '.artifacts[0].digest' <<< "${metadata}")" || return
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  artifact_id="$(jq -er '.artifacts[0].id' <<< "${metadata}")" || return
  [[ "${artifact_id}" =~ ^[0-9]+$ ]] || return 1
  mkdir -p "${GLUTEN_REPORTS_DIR}" "${cache}" || return
  # Downloaded native artifacts expire after two days. Remove older entries,
  # including partial transfers left behind by a cancelled job.
  find "${cache}" -ignore_readdir_race -maxdepth 1 -type f -mtime +3 -delete || return
  cached="${cache}/${artifact_id}-${digest#sha256:}.zip"
  exec 8> "${cached}.lock"
  flock --exclusive 8 || return
  valid_archive() {
    printf '%s  %s\n' "${digest#sha256:}" "$1" | sha256sum -c - \
      && unzip -tqq "$1"
  }
  if [[ -f "${cached}" ]] && valid_archive "${cached}"; then
    echo "Using cached artifact $1"
    unzip -oq "${cached}" -d "${GLUTEN_REPORTS_DIR}"
    return
  fi
  rm -f -- "${cached}"
  archive="$(mktemp "${cache}/${artifact_id}-partial-XXXXXX")" || return
  trap 'rm -f "${archive}"' EXIT
  for attempt in 1 2 3; do
    if curl -fsSL "${auth[@]}" --output "${archive}" "${url}" \
      && valid_archive "${archive}"; then
      mv -- "${archive}" "${cached}"
      unzip -oq "${cached}" -d "${GLUTEN_REPORTS_DIR}"
      return
    fi
    echo "Artifact $1 failed integrity validation (attempt ${attempt}/3)" >&2
  done
  return 1
)

ci_package_native() (
  source "${GLUTEN_DIR}/cpp/build/Release/generators/conanrun.sh"
  local runtime="${BOLT_ROOT}/.ci/gluten-runtime" library dependency
  local libraries=("${GLUTEN_DIR}"/cpp/build/package/bolt/linux/amd64/*.so)
  if [[ -f "${GLUTEN_REPORTS_DIR}/native-tests.json" ]]; then
    local executables=()
    mapfile -t executables < <(jq -r '.tests[].command[0]' "${GLUTEN_REPORTS_DIR}/native-tests.json" | sort -u)
    libraries+=("${executables[@]}")
  fi
  mkdir -p "${runtime}"
  for library in "${libraries[@]}"; do
    ldd "${library}" > "${GLUTEN_REPORTS_DIR}/$(basename "${library}").ldd"
    if grep -q 'not found' "${GLUTEN_REPORTS_DIR}/$(basename "${library}").ldd"; then return 1; fi
    while IFS= read -r dependency; do
      # System libraries and the compiler runtime are provided by the same image.
      [[ "${dependency}" == "${CONAN_HOME}/"* ]] || continue
      cp -L "${dependency}" "${runtime}/"
    done < <(ldd "${library}" | awk '$2 == "=>" && $3 ~ /^\// {print $3}')
  done
  (cd "${GLUTEN_DIR}/cpp/build/package/bolt/linux/amd64" && sha256sum *.so) \
    > "${GLUTEN_REPORTS_DIR}/native.sha256"
  (
    cd "${runtime}"
    shopt -s nullglob
    libraries=(./*)
    if ((${#libraries[@]})); then sha256sum "${libraries[@]}"; fi
  ) \
    > "${GLUTEN_REPORTS_DIR}/runtime.sha256"
  tar -C "${BOLT_ROOT}" -I 'gzip -1' -cf "${GLUTEN_REPORTS_DIR}/native.tgz" \
    gluten/cpp/build/package .ci/gluten-runtime
)

ci_restore_native() {
  tar -C "${BOLT_ROOT}" -xzf "${GLUTEN_REPORTS_DIR}/native.tgz"
  (cd "${GLUTEN_DIR}/cpp/build/package/bolt/linux/amd64" \
    && sha256sum -c "${GLUTEN_REPORTS_DIR}/native.sha256")
  if [[ -s "${GLUTEN_REPORTS_DIR}/runtime.sha256" ]]; then
    (cd "${BOLT_ROOT}/.ci/gluten-runtime" \
      && sha256sum -c "${GLUTEN_REPORTS_DIR}/runtime.sha256")
  fi
  for library in "${GLUTEN_DIR}"/cpp/build/package/bolt/linux/amd64/*.so; do
    ci_runtime ldd "${library}"
  done | tee "${GLUTEN_REPORTS_DIR}/native-linkage.log"
  if grep -q 'not found' "${GLUTEN_REPORTS_DIR}/native-linkage.log"; then return 1; fi
  # Gluten embeds the Bolt source identity in the packaged JAR.
  mkdir -p "${GLUTEN_DIR}/ep/build-bolt/build"
  ln -s "${BOLT_ROOT}" "${GLUTEN_DIR}/ep/build-bolt/build/bolt_ep"
  rm "${GLUTEN_REPORTS_DIR}/native.tgz"
}

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
