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

*Coming soon*: The easiest way to get the CLI scripts is to use the pre-built Docker
image (the image includes the necessary iRODS clients).

## Building and testing 

The tests require some services to be running (iRODS and possibly MySQL). See the
`docker-compose.yml` file for an example of how to achieve this using our Docker images. 

```commandline
pip install -r requirements.txt
pip install -r test-requirements.txt
pytest --it
```

## Logging to syslog 
When a script has the option `--logconf`, the user can specify a configuration file
with `handlers` that can log messages to different resources.
There is following an example of a configuration file `logging_syslog.conf` that logs to both:
- STDOUT
- syslog file, through `/dev/log` (default for Linux)

`logging_syslog.conf`
```
[loggers]
keys=root

[handlers]
keys=consoleHandler,syslogHandler

[formatters]
keys=consoleFormatter,syslogFormatter

[logger_root]
level=DEBUG
handlers=consoleHandler,syslogHandler

[handler_syslogHandler]
class=logging.handlers.SysLogHandler
level=ERROR
formatter=syslogFormatter
args=("/dev/log",)

[handler_consoleHandler]
class=StreamHandler
level=INFO
formatter=consoleFormatter
args=(sys.stdout,)

[formatter_consoleFormatter]
format=%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s

[formatter_syslogFormatter]
format=%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s
```

In the `consoleHandler`, the `level` option refers to the starting priority to consider. It means that if the priority is INFO it will log all messages to STDOUT starting from INFO including WARNING, ERROR and FATAL.
Whereas the `sysLogHandler` will log ERROR and FATAL to the `syslog` file. A formatter can be specified for each handler with different variable.
