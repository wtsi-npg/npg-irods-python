# syntax = docker/dockerfile:1.2

FROM ubuntu:bionic as builder

ARG PYTHON_VERSION=3.10

RUN echo "debconf debconf/frontend select Noninteractive" | debconf-set-selections && \
    apt-get update && \
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

COPY . .

ENV PYENV_ROOT "/app/.pyenv"
ENV PATH "${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

RUN ./docker/install_pyenv.sh

RUN pyenv install "$PYTHON_VERSION"
RUN pyenv global "$PYTHON_VERSION"

# This drives the choice of base system for this Dockerfile. iRODS 4.2.11 is not
# available for anything more recent than Ubuntu bionic, so that's what we use for
# the builder (above) and for the clients. This is also the reason we resort to
# pyenv to get a recent Python, rather than using a python-slim base image.
FROM ghcr.io/wtsi-npg/ub-18.04-irods-clients-4.2.11

RUN echo "debconf debconf/frontend select Noninteractive" | debconf-set-selections && \
    apt-get update && \
    apt-get install -q -y --no-install-recommends \
    ca-certificates \
    git \
    unattended-upgrades && \
    unattended-upgrade -d -v

ENV PYENV_ROOT "/app/.pyenv"
ENV PATH "${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"
ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY --from=builder /app /app

# Mount the .git directory to allow setuptools_scm to get the version
RUN --mount=source=.git,target=.git,type=bind \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

RUN useradd -l -m -s /bin/false appuser && mkdir /home/appuser/.irods

RUN apt-get remove -q -y unattended-upgrades \
    git && \
    apt-get autoremove -q -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER appuser

ENTRYPOINT ["/app/docker/docker-entrypoint.sh"]

CMD ["check-checksums", "--version"]
