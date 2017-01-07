# Cyclone

Cyclone is a timeseries data storage engine designed to be a drop-in replacement for the Carbon daemons and Graphite backend webservers. Cyclone is designed as a multithreaded server, eliminating the need for running multiple carbon-cache processes per machine (to utilize more CPU cores) and setting up a webserver.

Like most of my projects, this is only tested at a small scale (so far), so there may be unfound bugs or inefficiencies. Use at your own risk.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Run `make`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.11.

## Running

Cyclone is configured via a small JSON file. The comments in the example file (cyclone.conf.json) explain the details. Once that's all set up, just run `cyclone <config_filename>` (or use an init.d script to run it as a daemon).

## Testing

Python clients for all the interfaces are provided in the cyclone_client module. This consists of three submodules:
- `http_client.CycloneHTTPClient` is compatible with Cyclone servers, and also with Graphite servers if `format='pickle'` is given. This client supports only read queries.
- `stream_client.CycloneLineClient` and `stream_client.CyclonePickleClient` are both compatible with Cyclone and Graphite servers. These clients support only write queries.
- `thrift_client.CycloneThriftClient` is compatible only with Cyclone servers. It supports read and write queries, and uses persistent connections.

You can import cyclone_client and use the various client classes directly from there. See the docstrings on those classes for more information, or take a look at functional_test.py for usage examples.
