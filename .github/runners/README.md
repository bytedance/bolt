# Bolt runner deployment

Run the following commands in `.github/runners` on the Docker host. The full stack uses Docker Engine and Compose v2. The optional source cache also uses Python 3 and curl.

## Registry, Conan Server and GitHub runners

Set `ORGANIZATION_NAME` and `REPOSITORY_NAME` in `variables.env` to the GitHub owner and repository you administer. The Conan package server starts read-only when both admin variables are absent. For uploads, configure both `CONAN_ADMIN_USER` and `CONAN_ADMIN_PASSWORD` privately. An explicitly empty value counts as set.

For first registration, supply a repository-admin API credential to the runner services. For example, put this in a local `runner-auth.override.yml` and export the credential from a private shell. Keep the credential out of the shared `variables.env`, which is also used by the Conan service.

```yaml
services:
  runner-medium:
    environment:
      GITHUB_RUNNER_TOKEN: ${GITHUB_RUNNER_TOKEN:?Set the runner registration API credential}
  runner-small:
    environment:
      GITHUB_RUNNER_TOKEN: ${GITHUB_RUNNER_TOKEN:?Set the runner registration API credential}
```

```bash
read -rsp 'GitHub API credential: ' GITHUB_RUNNER_TOKEN
echo
export GITHUB_RUNNER_TOKEN
export COMPOSE_FILE=docker-compose.yml:runner-auth.override.yml
bash setup-runners.sh
```

`setup-runners.sh` builds the images, starts the registry, pushes the CI and Conan Server images, waits for the registry to serve both manifests, then starts Conan Server and the two runner services. The repository defaults are four `medium` runners and one `small` runner. Check the target repository's Actions runner page and look for `Listening for Jobs` in the container logs.

The runner image's entrypoint (`runner/entrypoint.py`) obtains a short-lived registration token from the repository runner API, calls the bundled `config.sh`, and starts `run.sh`. It reuses existing credentials in the same configured container. The upstream Compose file keeps those credentials in the container layer, so container recreation can require the API credential again. If an administrator supplies only a short-lived registration token, its consumer is `config.sh --url ... --token ...`; the Compose entrypoint expects the API credential. Coordinate registration scope and credentials with the target repository administrator.

## Arrow source archives

`arrow-cache.py` uses the Arrow and Substrait checksums pinned by the [arrow/15.0.1-oss recipe](https://github.com/bytedance/conan-center-index/blob/50b9807d8c919306824fbb3f4dc444d3b62f01b2/recipes/arrow/all/conandata.yml). It reuses valid archives from `/data/conan-source-cache/s` and fetches missing archives on the host. Override the parent directory with an absolute `BOLT_ARROW_CACHE_DIR` in the host shell. Conan binary packages are managed separately by the Conan package server.

Choose one deployment on a given host and port:

```bash
# Complete stack with the optional source service and runner bind mounts.
bash setup-runners.sh --with-arrow-cache

# Or only the source service, including warm-up. No runner credentials needed.
python3 arrow-cache.py serve
```

The standalone deployment uses Compose project `bolt-arrow-cache`. Both modes use `compose.arrow-cache.yml` and the stock Nginx image. HTTP binds to `127.0.0.1:9301` by default and serves digest-named archives at `/s/<sha256>`. The host operator can set `BOLT_ARROW_CACHE_BIND` and `BOLT_ARROW_CACHE_PORT`. To inspect the standalone service, use `docker compose -p bolt-arrow-cache -f compose.arrow-cache.yml ps`. For full-stack maintenance, include the base file and both optional overlays, as well as your registration override if applicable.

To carry the archives between machines, run the first two commands on the source and the last two in the same checkout on the destination after transferring the tar file:

```bash
python3 arrow-cache.py warm
python3 arrow-cache.py export /tmp/bolt-arrow-sources.tar

python3 arrow-cache.py import /tmp/bolt-arrow-sources.tar
python3 arrow-cache.py serve
```

## Public CI jobs

Use the original Bolt CI image with a fresh per-job `CONAN_HOME`. The cache-enabled stack makes these paths available inside each runner's Docker daemon. Bind them into the job container:

```yaml
container:
  image: bolt-registry:5000/bolt-ci:20260921
  volumes:
    - /opt/bolt-arrow-sources:/opt/bolt-arrow-sources:ro
    - /opt/bolt-arrow-cache.py:/opt/bolt-arrow-cache.py:ro
```

After configuring the job's Conan profile and before installing dependencies, run:

```bash
python3 /opt/bolt-arrow-cache.py configure-local
```

This verifies and copies just the two content-addressed archives into Conan's writable job-local source cache. The public cache step uses local paths, receives no source service URL or credentials, and prints a fixed success or failure message. Run host deployment commands on the host. The existing `scripts/configure-conan-sources-cache.py` is for clients that intentionally configure an authenticated HTTP source mirror.
