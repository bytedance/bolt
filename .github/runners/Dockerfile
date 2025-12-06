# Uses base debian image, installs curl, github CLI, less, and other tools
FROM debian:bookworm

ARG DEB_REGION=""
ARG HTTPS_PROXY=""

RUN [ "$DEB_REGION" != "" ] && \
    sed -i "s|http://deb.debian.org/debian|http://ftp.${DEB_REGION}.debian.org/debian|g" /etc/apt/sources.list.d/debian.sources \
    || true

# Update and install basic tools
RUN apt-get update && apt-get install -y \
    curl \
    less \
    git \
    unzip \
    jq \
    pipenv \
    && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI
RUN https_proxy=${HTTPS_PROXY} curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

# Install GitHub Actions runner
RUN arch=$(arch | sed s/aarch64/arm64/ | sed s/x86_64/x64/) && \
    mkdir -p /actions-runner \
    && cd /actions-runner \
    && https_proxy=${HTTPS_PROXY} curl -o actions-runner-linux-${arch}.tar.gz -L https://github.com/actions/runner/releases/download/v2.329.0/actions-runner-linux-${arch}-2.329.0.tar.gz \
    && tar xzf actions-runner-linux-${arch}.tar.gz \
    && rm actions-runner-linux-${arch}.tar.gz

RUN apt-get update && \
    /actions-runner/bin/installdependencies.sh && \
    rm -rf /var/lib/apt/lists/*

# Install docker
RUN apt-get update && apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
        && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
        $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
        && apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io \
        && sed -i 's|ulimit -Hn 524288|ulimit -n 65536|g' /etc/init.d/docker

RUN echo '{ "insecure-registries": ["bolt-registry:5000"] }' > /etc/docker/daemon.json

# Copy in pipenv files
COPY . /entrypoint
WORKDIR /entrypoint
RUN cd /entrypoint && pipenv install
ENTRYPOINT ["pipenv", "run", "python", "entrypoint.py"]
