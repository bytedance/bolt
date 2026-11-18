#!/usr/bin/env bash
set -euo pipefail

if [[ -n ${CONAN_ADMIN_USER+x} ]] || [[ -n ${CONAN_ADMIN_PASSWORD+x} ]]; then
  echo "Starting conan server with admin credentials"
  NEEDS_ADMIN=1
else
  echo "Starting read-only conan server"
  NEEDS_ADMIN=0
fi

if [[ "$NEEDS_ADMIN" == "1" ]]; then
  if [[ -z ${CONAN_ADMIN_USER+x} ]] || [[ -z ${CONAN_ADMIN_PASSWORD+x} ]]; then
    echo "ERROR: CONAN_ADMIN_USER and CONAN_ADMIN_PASSWORD environment variables must be set before running this script."
    exit 1
  fi
fi

SERVER_CONF_BASE="$CONAN_SERVER_HOME/server.conf.base"
SERVER_CONF="$CONAN_SERVER_HOME/server.conf"
cp "$SERVER_CONF_BASE" "$SERVER_CONF"

if [[ "$NEEDS_ADMIN" == "1" ]]; then
  cat << EOF >> "$SERVER_CONF"
[write_permissions]
*/*@*/*: $CONAN_ADMIN_USER

[users]
$CONAN_ADMIN_USER: $CONAN_ADMIN_PASSWORD
EOF

  echo "Server configuration updated successfully."
  echo "Admin user '$CONAN_ADMIN_USER' added with write permissions."
else
  cat << EOF >> "$SERVER_CONF"
[write_permissions]

[users]

EOF
fi

# Start Conan server
exec conan_server
