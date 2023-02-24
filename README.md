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
