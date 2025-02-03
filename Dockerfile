FROM --platform=linux/amd64 ubuntu:bionic AS builder

ARG PYTHON_VERSION=3.12

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -q -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    curl \
    git \
    libtool \
    pkg-config \
    libffi-dev \
    libbz2-dev \
    libssl-dev \
    zlib1g-dev \
    unattended-upgrades && \
    unattended-upgrade -d -v

WORKDIR /app

ENV PYENV_ROOT="/app/.pyenv"

# Put PYENV first to ensure we use the pyenv-installed Python
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"

COPY . /app

RUN /app/docker/install_pyenv.sh

RUN pyenv install "$PYTHON_VERSION"
RUN pyenv global "$PYTHON_VERSION"


# This drives the choice of base system for this Dockerfile. iRODS 4.2.11 is not
# available for anything more recent than Ubuntu bionic, so that's what we use for
# the builder (above) and for the clients. This is also the reason we resort to
# pyenv to get a recent Python, rather than using a python-slim base image.
FROM --platform=linux/amd64 ghcr.io/wtsi-npg/ub-18.04-baton-irods-4.2.11:5.0.0

ENV DEBIAN_FRONTEND=noninteractive

USER root

RUN apt-get update && \
    apt-get install -q -y --no-install-recommends \
    ca-certificates \
    git \
    unattended-upgrades && \
    unattended-upgrade -d -v

RUN echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" |\
    tee /etc/apt/sources.list.d/renci-irods.list && \
    apt-get update && \
    apt-get install -q -y --no-install-recommends \
    irods-icommands="4.2.11-1~$(lsb_release -sc)"

ENV PYENV_ROOT="/app/.pyenv"

# Put PYENV first to ensure we use the pyenv-installed Python
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=""

WORKDIR /app

COPY --from=builder /app /app

# Mount the .git directory to allow the build to get the version from git
RUN --mount=source=.git,target=.git,type=bind \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir . && \
    git status && \
    ls -al

RUN useradd -l -m -s /bin/false appuser && mkdir /home/appuser/.irods

RUN apt-get remove -q -y unattended-upgrades \
    git && \
    apt-get autoremove -q -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER appuser

ENTRYPOINT ["/app/docker/entrypoint.sh"]

CMD ["check-checksums", "--version"]
