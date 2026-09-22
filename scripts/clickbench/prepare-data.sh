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

set -euo pipefail

readonly source_url="${CLICKBENCH_DATA_URL:-https://datasets.clickhouse.com/hits_compatible/hits.parquet}"
readonly destination="${1:-${CLICKBENCH_DATA_DIR:-$PWD/clickbench-data}}"
readonly output="$destination/hits.parquet"
readonly source_marker="$output.url"
readonly parts="${CLICKBENCH_DOWNLOAD_PARTS:-1}"

mkdir -p "$destination"
parts_re='^[0-9]+$'
if [[ ! "$parts" =~ $parts_re ]] || [[ "$parts" -lt 1 ]]; then
  echo "CLICKBENCH_DOWNLOAD_PARTS must be a positive integer" >&2
  exit 1
fi

expected_size="$(curl --fail --silent --location --head "$source_url" \
  | awk 'tolower($1) == "content-length:" { size=$2 } END { gsub("\r", "", size); print size }')"
actual_size=0
if [[ -f "$output" ]]; then
  actual_size="$(stat --format='%s' "$output")"
fi
existing_source=""
if [[ -f "$source_marker" ]]; then
  existing_source="$(cat "$source_marker")"
fi

if [[ -n "$expected_size" && "$actual_size" == "$expected_size" ]]; then
  echo "$source_url" > "$source_marker"
  rm -rf "$output.parts"
  echo "ClickBench data already exists: $output ($actual_size bytes)"
  exit 0
fi

echo "Downloading ClickBench hits.parquet to $output"
if [[ "$parts" -gt 1 && -n "$expected_size" ]]; then
  range_status="$(
    curl --silent --location --range 0-0 --output /dev/null --write-out '%{http_code}' \
      "$source_url"
  )"
  if [[ "$range_status" != "206" ]]; then
    echo "CLICKBENCH_DOWNLOAD_PARTS requires a server that supports byte ranges" >&2
    exit 1
  fi

  part_dir="$output.parts"
  part_source_marker="$part_dir/url"
  tmp_output="$output.tmp"
  if [[ -f "$part_source_marker" ]] && [[ "$(cat "$part_source_marker")" != "$source_url" ]]; then
    rm -rf "$part_dir"
  fi
  mkdir -p "$part_dir"
  echo "$source_url" > "$part_source_marker"
  bytes_per_part=$(((expected_size + parts - 1) / parts))
  pids=()
  for ((part = 0; part < parts; ++part)); do
    start=$((part * bytes_per_part))
    end=$((start + bytes_per_part - 1))
    if [[ "$start" -ge "$expected_size" ]]; then
      break
    fi
    if [[ "$end" -ge "$expected_size" ]]; then
      end=$((expected_size - 1))
    fi
    part_file="$part_dir/part-$part"
    part_tmp="$part_file.tmp"
    expected_part_size=$((end - start + 1))
    if [[ -f "$part_file" ]] \
      && [[ "$(stat --format='%s' "$part_file")" == "$expected_part_size" ]]; then
      continue
    fi
    (
      actual_part_size=0
      if [[ -f "$part_tmp" ]]; then
        actual_part_size="$(stat --format='%s' "$part_tmp")"
      fi
      if [[ "$actual_part_size" -gt "$expected_part_size" ]]; then
        rm -f "$part_tmp"
        actual_part_size=0
      fi
      if [[ "$actual_part_size" -lt "$expected_part_size" ]]; then
        range_start=$((start + actual_part_size))
        curl --fail --location --silent --show-error --range "$range_start-$end" \
          --output - "$source_url" >> "$part_tmp"
      fi
      actual_part_size="$(stat --format='%s' "$part_tmp")"
      if [[ "$actual_part_size" != "$expected_part_size" ]]; then
        echo \
          "Part $part has $actual_part_size bytes, expected $expected_part_size" \
          >&2
        exit 1
      fi
      mv "$part_tmp" "$part_file"
    ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
  : > "$tmp_output"
  for ((part = 0; part < parts; ++part)); do
    part_file="$part_dir/part-$part"
    if [[ ! -f "$part_file" ]]; then
      echo "Missing downloaded part: $part_file" >&2
      exit 1
    fi
    cat "$part_file" >> "$tmp_output"
  done
  actual_size="$(stat --format='%s' "$tmp_output")"
  if [[ "$actual_size" != "$expected_size" ]]; then
    echo "Downloaded file has $actual_size bytes, expected $expected_size" >&2
    exit 1
  fi
  mv "$tmp_output" "$output"
  echo "$source_url" > "$source_marker"
  rm -rf "$part_dir"
  echo "Downloaded $(stat --format='%s' "$output") bytes"
  exit 0
fi

if [[ "$actual_size" -gt 0 ]]; then
  if [[ "$existing_source" == "$source_url" ]]; then
    echo "Resuming from $actual_size of ${expected_size:-unknown} bytes"
  else
    tmp_output="$output.tmp"
    echo "Existing partial file has no matching source marker; restarting download into $tmp_output"
    rm -f "$tmp_output"
    curl --fail --location --output "$tmp_output" "$source_url"
    mv "$tmp_output" "$output"
    echo "$source_url" > "$source_marker"
    echo "Downloaded $(stat --format='%s' "$output") bytes"
    exit 0
  fi
fi
if ! curl --fail --location --continue-at - --output "$output" "$source_url"; then
  tmp_output="$output.tmp"
  echo "Resume failed; restarting download into $tmp_output"
  rm -f "$tmp_output"
  curl --fail --location --output "$tmp_output" "$source_url"
  mv "$tmp_output" "$output"
fi
echo "$source_url" > "$source_marker"
echo "Downloaded $(stat --format='%s' "$output") bytes"
