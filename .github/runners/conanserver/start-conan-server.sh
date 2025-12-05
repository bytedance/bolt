#!/usr/bin/env bash
set -euo pipefail
# Validate environment variables
if [[ -z ${CONAN_ADMIN_USER+x} ]] || [[ -z ${CONAN_ADMIN_PASSWORD+x} ]]; then
    echo "ERROR: CONAN_ADMIN_USER and CONAN_ADMIN_PASSWORD environment variables must be set before running this script."
    exit 1
fi

# Configuration file path
SERVER_CONF_BASE="$CONAN_SERVER_HOME/server.conf.base"
SERVER_CONF="$CONAN_SERVER_HOME/server.conf"
cp "$SERVER_CONF_BASE" "$SERVER_CONF"

cat <<EOF >> "$SERVER_CONF"
[write_permissions]
*/*@*/*: $CONAN_ADMIN_USER

[users]
$CONAN_ADMIN_USER: $CONAN_ADMIN_PASSWORD
EOF

echo "Server configuration updated successfully."
echo "Admin user '$CONAN_ADMIN_USER' added with write permissions."

# Start Conan server
conan_server