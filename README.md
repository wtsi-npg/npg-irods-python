# npg-irods-python

[![Unit tests](https://github.com/wtsi-npg/npg-irods-python/actions/workflows/ci.yml/badge.svg)](https://github.com/wtsi-npg/npg-irods-python/actions/workflows/ci.yml)

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

## Building and testing 

The tests require some services to be running (iRODS and possibly MySQL). See the
`docker-compose.yml` file for an example of how to achieve this using our Docker images. 

```commandline
pip install -r requirements.txt
pip install -r test-requirements.txt
pytest --it
```

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
