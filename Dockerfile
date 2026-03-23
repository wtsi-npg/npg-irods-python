FROM --platform=linux/amd64 ubuntu:bionic AS builder

ARG PYTHON_VERSION=3.14

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -q -y --no-install-recommends \
    autoconf \
    automake \
    ca-certificates \
    curl \
    git \
    libtool \
    make \
    pkg-config \
    rhash \
    libffi-dev \
    libbz2-dev \
    libffi-dev \
    libncurses-dev \
    libreadline-dev \
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

ARG SQLITE_AUTOCONF_VERSION=3510300
ARG SQLITE_AUTOCONF_SHA3_256=581215771b32ea4c4062e6fb9842c4aa43d0a7fb2b6670ff6fa4ebb807781204

ENV SQLITE_PREFIX="/opt/sqlite"
ENV CPPFLAGS="-I${SQLITE_PREFIX}/include"
ENV LDFLAGS="-L${SQLITE_PREFIX}/lib -Wl,-rpath,${SQLITE_PREFIX}/lib"
ENV LD_LIBRARY_PATH="${SQLITE_PREFIX}/lib"
ENV PKG_CONFIG_PATH="${SQLITE_PREFIX}/lib/pkgconfig"

ENV MAKE_OPTS="-j 8"

RUN curl -fsSLO "https://sqlite.org/2026/sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz" && \
    rhash --sha3-256 "sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz" | grep "$SQLITE_AUTOCONF_SHA3_256" && \
    tar -xzf "sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz" && \
    cd "sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}" && \
    ./configure --prefix="$SQLITE_PREFIX" && \
    make && \
    make install && \
    cd /app

RUN pyenv install "$PYTHON_VERSION"
RUN pyenv global "$PYTHON_VERSION"


# This drives the choice of base system for this Dockerfile. iRODS 4.2.11 is not
# available for anything more recent than Ubuntu bionic, so that's what we use for
# the builder (above) and for the clients. This is also the reason we resort to
# pyenv to get a recent Python, rather than using a python-slim base image.
FROM --platform=linux/amd64 ghcr.io/wtsi-npg/ub-18.04-baton-irods-4.2.11:6.0.0

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
ENV LD_LIBRARY_PATH="/usr/local/lib"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=""

WORKDIR /app

COPY --from=builder /app /app
COPY --from=builder /opt/sqlite/lib/libsqlite3.so* /usr/local/lib/

RUN ldconfig

# Mount the .git directory to allow the build to get the version from git
RUN --mount=source=.git,target=.git,type=bind \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir . && \
    git status && \
    ls -al

RUN id -u appuser || useradd -l -m -s /bin/false appuser && mkdir /home/appuser/.irods

RUN apt-get remove -q -y unattended-upgrades \
    git && \
    apt-get autoremove -q -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER appuser

ENTRYPOINT ["/app/docker/entrypoint.sh"]

CMD ["check-checksums", "--version"]
