# Cyclone

Cyclone is a timeseries data storage engine designed to be a drop-in replacement for the Carbon daemons and Graphite backend webservers. Cyclone is designed as a multithreaded server, eliminating the need for running multiple carbon-cache processes per machine (to utilize more CPU cores) and setting up a webserver.

Like most of my projects, this is only tested at a small scale (so far), so there may be unfound bugs or inefficiencies. Use at your own risk.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Run `make`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.13 and Ubuntu 16.04.

## Running

Cyclone is configured via a small JSON file. The comments in the example file (cyclone.conf.json) explain the details. Once that's all set up, just run `cyclone <config_filename>` (or use an appropriate daemonizer).

Cyclone is compatible with the Graphite webapp. You can even run the Graphite webapp directly in front of Cyclone by setting Graphite's `CLUSTER_SERVERS = ['localhost:5050']` (or any port in `http_listen` from Cyclone's configuration). Cyclone will handle the clustering logic if applicable; the Graphite webapp only needs to talk to one of the Cyclone instances in the cluster.

## Testing

Python clients for all the interfaces are provided in the cyclone_client module. This consists of three submodules:
- `http_client.CycloneHTTPClient` is compatible with Cyclone servers, and also with Graphite servers if `format='pickle'` is given. This client supports only read queries.
- `stream_client.CycloneLineClient` and `stream_client.CyclonePickleClient` are both compatible with Cyclone and Graphite servers. These clients support only write queries.
- `thrift_client.CycloneThriftClient` is compatible only with Cyclone servers. It supports read and write queries, and uses persistent connections.

You can import cyclone_client and use the various client classes directly from there. See the docstrings on those classes for more information, or take a look at functional_test.py for usage examples.

You can also use the shell interface by connecting to the shell port with telnet or nc.
