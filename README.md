# npg-irods-python

[![Unit tests](https://github.com/wtsi-npg/npg-irods-python/actions/workflows/run-tests.yml/badge.svg)](https://github.com/wtsi-npg/npg-irods-python/actions/workflows/run-tests.yml)

## Overview

This repository is the home of application code used by NPG to manage data and 
metadata in WSI [iRODS](https://irods.org).

It includes:

- iRODS CLI utilities
  - Metadata verification and repair.
  - Checksum verification and repair.
  - Replica verification and repair.
  - Safe bulk copy.
  - Safe bulk deletion.

- General purpose API
  - Managing standard WSI iRODS metadata.

- Analysis platform-specific API and CLI utilities
  - Managing Oxford Nanopore metadata and permissions.


## Installing

See [Building and testing](#building-and-testing).

The easiest way to get the CLI scripts is to use the pre-built Docker image (the image
includes the necessary iRODS clients):

```shell
# Latest release
docker pull ghcr.io/wtsi-npg/npg-irods-python:latest

# Specific version
docker pull ghcr.io/wtsi-npg/npg-irods-python:1.1.0
```

## Running tests

### Running directly on your local machine

To run the tests locally, you will need to have the `irods` clients installed (`icommands`
and `baton`, which means your local machine must be either be running Linux, or have
containerised versions of these tools installed and runnable via proxy wrappers of the
same name, to emulate the Linux environment.

You will also need to have a working iRODS server to connect to.

With this in place, you can run the tests with the following command:

    pytest --it

### Running in a container

The tests can be run in a container, which requires less setup and will be less likely
to be affected by your local environment. A Docker Compose file is provided to run the
tests in a Linux container, against containerised iRODS and MySQL servers.

To run the tests in a container, you will need to have Docker installed.

With this in place, you can run the tests with the following command:

    docker-compose run app pytest --it

There will be a delay the first time this is run because the Docker image will be built.
To pre-build the image, you can run:

    docker-compose build

## Logging

### Structured logging

Most of the scripts have a CLI option `--json` to enable structured logging in JSON.
This is preferred when the scripts are run as a service, particularly when forwarding
logs to an aggregator (such as ELK) because it allows more effective filtering than
unstructured messages.

### Logging configuration

This package uses the standard Python logging library to deliver log messages. When a
script has the option `--log-config`, the user can specify a configuration file
to modify logging behaviour e.g. to set log levels and add new log destinations. 

The configuration file must be JSON, in the form of a standard logging [configuration 
dictionary](https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema). 

An example configuration is provided in the file `logging.json`:

```json
{
  "version": 1,
  "disable_existing_loggers": false,
  "formatters": {
    "stderr": {
      "format": "%(message)s"
    },
    "syslog": {
      "format": "%(message)s"
    }
  },
  "handlers": {
    "stderr": {
      "class": "logging.StreamHandler",
      "level": "INFO",
      "formatter": "stderr",
      "stream": "ext://sys.stderr"
    },
    "syslog": {
      "class": "logging.handlers.SysLogHandler",
      "level": "ERROR",
      "formatter": "syslog",
      "address": "/dev/log"
    }
  },
  "root": {
    "level": "ERROR",
    "handlers": [
      "stderr",
      "syslog"
    ]
  }
}
```


In the `stderr` handler, the `level` option refers to the starting priority to consider.
Its priority of INFO means that it will log all messages to STDERR starting from INFO,
including WARNING, ERROR and FATAL. Whereas the `syslog` handler will log ERROR and
FATAL to the syslog.

A formatter can be specified for each handler with different variable. However, as we
rely on `structlog` to pre-format the messages, we simply forward the pre-formatted
string.
